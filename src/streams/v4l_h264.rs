#![cfg(all(target_os = "linux", feature = "v4l"))]

use std::time::Duration;

use anyhow::{bail, Result};

use bytes::{Bytes, BytesMut};

use crate::encoders::{EncoderConfig, EncoderType, FfmpegOptions, InputType, VideoEncoder};

use ffmpeg_next::util::format::Pixel as AvPixel;

use tracing::{debug, error};

use v4l::buffer::Type;
use v4l::io::traits::CaptureStream;
use v4l::prelude::*;
use v4l::video::traits::Capture;

use std::fs::File;
use std::{io, io::Write};

use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TrySendError;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::io::StreamReader;

// Adaptive bitrate window
const ABR_WINDOW: std::time::Duration = std::time::Duration::from_secs(1);
// Decrease rate on congestion
const ABR_DECREASE: f64 = 0.75;
// Number of clean windows before bitrate increase
const ABR_CLEAN_WINDOWS: u32 = 3;

// TODO: make this more generic so you can have a v4l stream with
// different encoder types (e.g AV1)

fn fourcc_to_input_type(fourcc: v4l::FourCC) -> Result<InputType> {
    match &fourcc.repr[..] {
        b"BGR4" => Ok(AvPixel::BGR32),
        b"BGR3" => Ok(AvPixel::BGR24),
        b"RGB3" => Ok(AvPixel::RGB24),
        b"YUYV" => Ok(AvPixel::YUYV422),
        b"UYVY" => Ok(AvPixel::UYVY422),
        b"YV12" => Ok(AvPixel::YUV420P),
        b"NV12" => Ok(AvPixel::NV12),
        b"NV21" => Ok(AvPixel::NV21),
        b"MJPG" => Ok(AvPixel::YUVJ420P),
        b"GREY" => Ok(AvPixel::GRAY8),
        _ => bail!("Unsupported v4l FourCC type: {:?}", fourcc),
    }
}

#[derive(Clone)]
pub struct LoadingImage {
    pub data: Vec<u8>,
    pub input_width: u32,
    pub input_height: u32,
    pub input_type: InputType,
}

pub struct V4lH264Config {
    pub output_width: u32,
    pub output_height: u32,
    pub bitrate: usize,
    pub bitrate_min: usize,
    pub target_fps: Option<u32>,
    pub video_dev: String,
    pub v4l_fourcc: v4l::FourCC,
    pub loading_image: Option<LoadingImage>,
}

pub struct V4lH264Stream {}

impl V4lH264Stream {
    pub fn new(
        cfg: V4lH264Config,
        ffmpeg_opts: FfmpegOptions,
    ) -> Result<StreamReader<ReceiverStream<Result<BytesMut, io::Error>>, BytesMut>> {
        let input_type = fourcc_to_input_type(cfg.v4l_fourcc)?;
        // only allow 10 frames to be buffered
        // TODO: maybe make this a configurable option
        let (tx, rx) = mpsc::channel::<Result<BytesMut, io::Error>>(10);

        std::thread::spawn(move || {
            // TODO: better error handling, should close the channel correctly instead of exploding
            let cached_loading_frames = cfg.loading_image.as_ref().map(|loading_image| {
                let loading_ec = EncoderConfig {
                    input_width: loading_image.input_width,
                    input_height: loading_image.input_height,
                    output_width: cfg.output_width,
                    output_height: cfg.output_height,
                    enc_type: EncoderType::X264,
                    input_type: loading_image.input_type,
                    opts: vec![
                        ("framerate".into(), "10".into()),
                        ("preset".into(), "medium".into()),
                        ("tune".into(), "stillimage".into()),
                        ("x264-params".into(), "repeat-headers=1:keyint=1:min-keyint=1:scenecut=0".into()),
                        ("g".into(), "1".into()),
                        ("b".into(), "1000000".into()),
                        ("bf".into(), "0".into()),
                    ],
                };

                let mut f = File::create("loading-video.h264").unwrap();

                let mut loading_encoder = VideoEncoder::new(loading_ec).unwrap();
                let mut nal_frames = Vec::new();
                loop {
                    if let Some(encoded_frame) = loading_encoder
                        .encode_raw(Some(0), &loading_image.data)
                        .unwrap()
                    {
                        nal_frames.push(encoded_frame.data.freeze());
                        break;
                    }
                }

                for encoded_frame in loading_encoder.drain().unwrap() {
                    let frame = encoded_frame.data.clone().freeze();
                    f.write_all(&frame);
                    f.flush();
                    nal_frames.push(frame);
                }

                tracing::error!("Total Loaded Image NAL frames: {}", nal_frames.len());

                nal_frames
            });

            let mut v4l_dev = Device::with_path(&cfg.video_dev)
                .expect("Failed to open v4l device. Device may not exist.");
            loop {
                // block until the v4l_device is up
                if cfg.v4l_fourcc == v4l_dev.format().unwrap().fourcc {
                    break;
                } else {
                    tracing::error!(
                        "{} doesn't have requested FourCC {}!",
                        &cfg.video_dev.as_str(),
                        cfg.v4l_fourcc
                    );
                    if let Some(frames) = cached_loading_frames.as_ref() {
                        if !send_loading_frames(&tx, frames) {
                            return;
                        }
                    } else {
                        std::thread::sleep(std::time::Duration::from_secs(1));
                    }

                    v4l_dev = Device::with_path(&cfg.video_dev)
                        .expect("Failed to open v4l device. Device may not exist.");
                }
            }

            let mut stream = MmapStream::new(&v4l_dev, Type::VideoCapture).unwrap();

            let format = v4l_dev.format().unwrap();
            debug!("V4L Format: {:?}", format);
            let mut pts: i64 = 0;

            // cap fps based on config so we don't overrun the link
            let target_fps = cfg.target_fps.filter(|&f| f > 0);
            let min_frame_interval = target_fps.map(|f| Duration::from_secs_f64(1.0 / f as f64));
            let mut last_encoded: Option<std::time::Instant> = None;
            let encoder_fps = target_fps.unwrap_or(15);

            // Create encoder at given bitrate
            let make_encoder = |bitrate: usize| {
                let mut opts: FfmpegOptions = vec![
                    ("framerate".into(), encoder_fps.to_string()),
                    ("b".into(), bitrate.to_string()),
                    ("bf".into(), "0".into()),
                ];
                opts.extend(ffmpeg_opts.clone());
                opts.push(("x264-params".into(), "repeat-headers=1".into()));
                VideoEncoder::new(EncoderConfig {
                    input_width: format.width,
                    input_height: format.height,
                    output_width: cfg.output_width,
                    output_height: cfg.output_height,
                    enc_type: EncoderType::X264,
                    input_type,
                    opts,
                })
                .unwrap()
            };

            let bitrate_min = cfg.bitrate_min.min(cfg.bitrate);
            let bitrate_max = cfg.bitrate;
            let abr_step = ((bitrate_max - bitrate_min) / 8).max(1);
            let mut curr_bitrate = bitrate_max;
            let mut encoder = make_encoder(curr_bitrate); // dynamic bitrate encoder
            let mut abr_window_start = std::time::Instant::now();
            let mut abr_window_drops: u64 = 0;
            let mut abr_clean_windows: u32 = 0;
            let mut dropped: u64 = 0;

            loop {
                // TODO: Better error handling
                match stream.next() {
                    Ok((m_buf, meta)) => {
                        // skip frames arriving faster than target fps
                        if let Some(interval) = min_frame_interval {
                            if last_encoded.is_some_and(|t| t.elapsed() < interval) {
                                continue;
                            }
                            last_encoded = Some(std::time::Instant::now());
                        }
                        let bytesused = meta.bytesused as usize;
                        // debug!("V4L bytesused: {}", meta.bytesused);
                        if let Some(encoded_frame) =
                            encoder.encode_raw(Some(pts), &m_buf[..bytesused]).unwrap()
                        {
                            // Try to send the frame
                            match tx.try_send(Ok(encoded_frame.data)) {
                                Ok(()) => {
                                    if dropped > 0 {
                                        error!(
                                            "{}: consumer caught up, dropped {} frames",
                                            &cfg.video_dev, dropped
                                        );
                                        dropped = 0;
                                    }
                                }
                                Err(TrySendError::Full(_)) => { 
                                    dropped += 1; // drop the frame if full
                                    abr_window_drops += 1; // dropped frame this window
                                }
                                Err(TrySendError::Closed(_)) => return,
                            }
                        }
                        pts += 1;

                        // update bitrate
                        if bitrate_min < bitrate_max && abr_window_start.elapsed() >= ABR_WINDOW {
                            // we have dropped frames in this window, decrease
                            let target = if abr_window_drops > 0 {
                                abr_clean_windows = 0;
                                ((curr_bitrate as f64 * ABR_DECREASE) as usize).max(bitrate_min)
                            } else {
                                // no drops, increase
                                abr_clean_windows += 1;
                                if abr_clean_windows >= ABR_CLEAN_WINDOWS {
                                    abr_clean_windows = 0;
                                    (curr_bitrate + abr_step).min(bitrate_max)
                                } else {
                                    curr_bitrate
                                }
                            };
                            if target != curr_bitrate {
                                curr_bitrate = target;
                                encoder = make_encoder(curr_bitrate); // dynamic bitrate encoder
                                error!("{}: adaptive bitrate -> {} bps", &cfg.video_dev, curr_bitrate);
                            }
                            abr_window_start = std::time::Instant::now();
                            abr_window_drops = 0;
                        } 
                    }
                    Err(e) => {
                        if let Some(error_code) = e.raw_os_error() {
                            if error_code == 5 {
                                error!(
                                    "Got I/O Error: {} for {}. Retrying in 1 second",
                                    error_code, &cfg.video_dev
                                );
                                if let Some(frames) = cached_loading_frames.as_ref() {
                                    if !send_loading_frames(&tx, frames) {
                                        return;
                                    }
                                } else {
                                    std::thread::sleep(std::time::Duration::from_secs(1));
                                }
                            } else {
                                panic!("Unrecoverable OS Error: {}", e);
                            }
                        }
                    }
                }
            }
        });

        Ok(StreamReader::new(ReceiverStream::new(rx)))
    }
}

fn send_loading_frames(tx: &mpsc::Sender<Result<BytesMut, io::Error>>, frames: &[Bytes]) -> bool {
    if frames.is_empty() {
        return true;
    }

    for frame in frames {
        // Drop frame if full
        match tx.try_send(Ok(BytesMut::from(frame.as_ref()))) {
            Ok(()) | Err(TrySendError::Full(_)) => {}
            Err(TrySendError::Closed(_)) => return false,
        }

        std::thread::sleep(std::time::Duration::from_millis(250));
    }

    true
}
