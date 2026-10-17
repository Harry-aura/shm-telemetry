#!/usr/bin/env node
'use strict';

const net = require('node:net');

function fail(msg) {
  console.error(`validate_bridge: FAIL: ${msg}`);
  process.exit(1);
}

const args = process.argv.slice(2);
const sockPath = args[0];
const wantFrames = Number.parseInt(args[1] || '30', 10);
const requireWriters = args.includes('--require-writers');

if (!sockPath || !Number.isInteger(wantFrames) || wantFrames < 1) {
  console.error('usage: validate_bridge.js <socket_path> [frame_count] [--require-writers]');
  process.exit(2);
}

function isNum(v) {
  return typeof v === 'number' && Number.isFinite(v);
}

let frames = 0;
let prevEnq = null;
let maxFilled = 0;
let sawProducer = false;
let sawConsumer = false;
let sawAliveProducer = false;
let crcOkTotal = 0;
let crcBadTotal = 0;
let firstLine = null;
let lastLine = null;
let buffer = '';
let done = false;

function validateFrame(obj, raw) {
  if (typeof obj !== 'object' || obj === null) fail('frame is not an object');
  const required = ['t', 'frame_seq', 'interval_ms', 'attached', 'metrics', 'writers', 'slot_states', 'slots'];
  for (const key of required) {
    if (!(key in obj)) fail(`missing field '${key}'`);
  }
  if (!isNum(obj.t) || !isNum(obj.frame_seq) || !isNum(obj.interval_ms)) {
    fail('t/frame_seq/interval_ms must be numbers');
  }
  if (typeof obj.attached !== 'boolean') fail('attached must be a boolean');
  if (!Array.isArray(obj.writers)) fail('writers must be an array');
  if (!Array.isArray(obj.slots)) fail('slots must be an array');
  if (typeof obj.slot_states !== 'string') fail('slot_states must be a base64 string');

  const m = obj.metrics;
  if (typeof m !== 'object' || m === null) fail('metrics must be an object');
  const metricKeys = [
    'produced_total', 'consumed_total', 'backpressure_events', 'crc_errors', 'seq_errors',
    'min_latency_ns', 'max_latency_ns', 'avg_latency_ns', 'enqueue_pos', 'dequeue_pos',
    'occupancy', 'capacity', 'refcount',
  ];
  for (const key of metricKeys) {
    if (!isNum(m[key])) fail(`metrics.${key} must be a number`);
  }
  if (!Array.isArray(m.latency_buckets) || m.latency_buckets.length !== 5) {
    fail('metrics.latency_buckets must be an array of 5 numbers');
  }
  for (const b of m.latency_buckets) {
    if (!isNum(b)) fail('latency bucket entries must be numbers');
  }

  if (!obj.attached) {
    return { attached: false };
  }

  let decoded;
  try {
    decoded = Buffer.from(obj.slot_states, 'base64');
  } catch {
    fail('slot_states is not valid base64');
  }
  if (decoded.length !== m.capacity) {
    fail(`slot_states decoded ${decoded.length} bytes but capacity is ${m.capacity}`);
  }
  let filled = 0;
  for (let i = 0; i < decoded.length; i++) {
    const s = decoded[i];
    if (s < 0 || s > 3) fail(`slot_states[${i}] = ${s} out of range 0..3`);
    if (s === 1) filled++;
  }
  if (filled > 1024) fail(`filled slot count ${filled} exceeds capacity 1024`);
  if (filled > maxFilled) maxFilled = filled;

  for (const w of obj.writers) {
    if (typeof w !== 'object' || w === null) fail('writer entry is not an object');
    if (w.role !== 'producer' && w.role !== 'consumer') fail(`bad writer role '${w.role}'`);
    if (!isNum(w.slot) || !isNum(w.pid) || typeof w.alive !== 'boolean' || !isNum(w.produced)) {
      fail('writer entry has wrong field types');
    }
    if (w.role === 'producer') {
      sawProducer = true;
      if (w.alive) sawAliveProducer = true;
    } else {
      sawConsumer = true;
    }
  }

  for (const s of obj.slots) {
    if (typeof s !== 'object' || s === null) fail('slot entry is not an object');
    for (const key of ['idx', 'pos', 'msg_id', 'sender_pid', 'producer_idx', 'age_ns', 'payload_len', 'crc32']) {
      if (!isNum(s[key])) fail(`slot.${key} must be a number`);
    }
    if (typeof s.crc_ok !== 'boolean') fail('slot.crc_ok must be a boolean');
    if (typeof s.preview !== 'string') fail('slot.preview must be a string');
    if (s.crc_ok) crcOkTotal++;
    else crcBadTotal++;
  }

  if (prevEnq !== null && m.enqueue_pos <= prevEnq) {
    fail(`enqueue_pos not strictly increasing (${prevEnq} -> ${m.enqueue_pos})`);
  }
  prevEnq = m.enqueue_pos;
  return { attached: true, enq: m.enqueue_pos, occ: m.occupancy, filled };
}

const socket = net.connect({ path: sockPath });
socket.setEncoding('utf8');

const timer = setTimeout(() => {
  console.error(`validate_bridge: FAIL: timeout waiting for ${wantFrames} frames (got ${frames})`);
  process.exit(1);
}, 15000);

socket.on('connect', () => {
  console.error(`validate_bridge: connected to ${sockPath}, waiting for ${wantFrames} frames`);
});

socket.on('data', (chunk) => {
  buffer += chunk;
  let idx;
  while ((idx = buffer.indexOf('\n')) >= 0) {
    const line = buffer.slice(0, idx);
    buffer = buffer.slice(idx + 1);
    if (line.length === 0) continue;
    let obj;
    try {
      obj = JSON.parse(line);
    } catch (err) {
      fail(`frame is not valid JSON: ${String(err)}`);
    }
    const info = validateFrame(obj, line);
    frames++;
    if (firstLine === null) firstLine = line;
    lastLine = line;
    if (frames >= wantFrames && !done) {
      done = true;
      clearTimeout(timer);
      finish(info);
      return;
    }
  }
});

socket.on('error', (err) => {
  fail(`socket error: ${String(err)}`);
});
socket.on('close', () => {
  if (!done) fail(`socket closed after ${frames}/${wantFrames} frames`);
});

function finish() {
  if (!sawProducer) fail('no producer writer slot observed');
  if (!sawConsumer) fail('no consumer writer slot observed');
  if (requireWriters && !sawAliveProducer) fail('no alive producer writer observed');
  if (maxFilled < 1) fail('filled slot count was never >= 1 across all frames');
  if (crcBadTotal > 0) fail(`${crcBadTotal} slot snapshots had crc_ok=false`);
  console.log(`validate_bridge: PASS: ${frames} frames, enqueue_pos strictly increasing up to ${prevEnq}`);
  console.log(`validate_bridge: filled slots max=${maxFilled} (<=1024), crc_ok=${crcOkTotal} crc_bad=${crcBadTotal}`);
  console.log(`validate_bridge: writers producer=${sawProducer} consumer=${sawConsumer}`);
  console.log(`validate_bridge: first frame (truncated): ${firstLine.slice(0, 160)}...`);
  console.log(`validate_bridge: last  frame (truncated): ${lastLine.slice(0, 160)}...`);
  socket.end();
  process.exit(0);
}
