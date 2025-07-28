/**********************************************************************
 * download‑and‑unpack.js
 * --------------------------------------------------------------------
 *   node download‑and‑unpack.js
 *********************************************************************/

import {
    S3Client,
    ListObjectsV2Command,
    GetObjectCommand,
  } from '@aws-sdk/client-s3';
  import fs from 'node:fs';
  import fsp from 'node:fs/promises';
  import path from 'node:path';
  import { pipeline } from 'node:stream/promises';
  import unzipper from 'unzipper';
  import zlib from 'node:zlib';
  import pLimit from 'p-limit';
  
  /* ──────────────── CONFIG ────────────────────────────────────────── */
  const BUCKET   = '110ec58a-a0f2-4ac4-8393-c866d813b8d8-1383'; // the bucket part
  const PREFIXES = {
    new: '110ec58a-a0f2-4ac4-8393-c866d813b8d8#Account#0001/data/json/adsDataIPL2025/1749166725994/JITDeltaOAD/adsDataIPL2025JITDelta32/1752643242042/JIT_DELTA_ROW_VECTOR/',
    // old: '110ec58a-a0f2-4ac4-8393-c866d813b8d8#Account#0001/data/json/adsDataIPL2025/1749090726431/JITDeltaOAD/adsDataIPL2025JITDelta15/1744100296866/JIT_DELTA_ROW_VECTOR/',
  };
  
  const DEST_ROOT = path.resolve('data');          // local directory root
  const MAX_PAR   = 8;                             // concurrent downloads
  
  /* ──────────────── BOILERPLATE ───────────────────────────────────── */
  const s3    = new S3Client({
    region: "us-east-1",
    credentials: {
      accessKeyId: process.env?.accessKeyId   ,
      secretAccessKey: process.env?.secretAccessKey,
    },
  });                  // region picked up from env
  const limit = pLimit(MAX_PAR);
  
  async function ensureDir(p) { await fsp.mkdir(p, { recursive: true }); }
  
  /* ──────────────── MAIN ──────────────────────────────────────────── */
  (async () => {
    for (const [tag, prefix] of Object.entries(PREFIXES)) {
      const keys = await listAllKeys(prefix);
      console.log(`[${tag}] ${keys.length} objects`);
  
      await Promise.all(
        keys.map(key => limit(() => grabOne(tag, key)))
      );
    }
    console.log('✅  Done');
  })().catch(err => { console.error(err); process.exit(1); });
  
  /* ──────────────── HELPERS ───────────────────────────────────────── */
  async function listAllKeys(prefix) {
    const keys = [];
    let token;
    do {
      const res = await s3.send(
        new ListObjectsV2Command({
          Bucket: BUCKET,
          Prefix: prefix,
          ContinuationToken: token,
        })
      );
      res.Contents?.forEach(obj => keys.push(obj.Key));
      token = res.IsTruncated ? res.NextContinuationToken : undefined;
    } while (token);
    return keys;
  }
  
  async function grabOne(tag, key) {
    const relPath = key.replace(PREFIXES[tag], '');        // strip leading prefix
    const destDir = path.join(DEST_ROOT, tag, path.dirname(relPath));
    await ensureDir(destDir);
  
    const obj = await s3.send(new GetObjectCommand({ Bucket: BUCKET, Key: key }));
    const tmp = path.join(destDir, path.basename(relPath));
  
    // save the raw bytes first
    await pipeline(obj.Body, fs.createWriteStream(tmp));
  
    // auto‑unpack if needed
    if (tmp.endsWith('.zip'))      await unzipFile(tmp, destDir);
    else if (tmp.endsWith('.gz'))  await gunzipFile(tmp, destDir);
  }
  
  async function unzipFile(zipPath, destDir) {
    await pipeline(
      fs.createReadStream(zipPath),
      unzipper.Extract({ path: destDir })
    );
  
    // Retry file deletion with exponential backoff
    const maxRetries = 5;
    let lastError;
  
    for (let i = 0; i < maxRetries; i++) {
      try {
        await new Promise(resolve => setTimeout(resolve, 100 * Math.pow(2, i))); // 100, 200, 400, 800, 1600ms
        await fsp.unlink(zipPath);
        return; // Success!
      } catch (error) {
        lastError = error;
        if (error.code !== 'EBUSY' && error.code !== 'EPERM') {
          throw error; // Non-retryable error
        }
        console.warn(`Retry ${i + 1}/${maxRetries} for ${zipPath}: ${error.code}`);
      }
    }
  
    throw lastError; // All retries failed
  }
  
  async function gunzipFile(gzPath, destDir) {
    const outPath = gzPath.replace(/\.gz$/i, '');
    await pipeline(
      fs.createReadStream(gzPath),
      zlib.createGunzip(),
      fs.createWriteStream(outPath)
    );
    await fsp.unlink(gzPath);
  }
  