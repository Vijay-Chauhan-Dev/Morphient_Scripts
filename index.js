let listOld = [];
let listNew = [];

// ─── Work ──────────────────────────────────────────────────────────────
// const output = [];
// let count = 0;
// let timeStart = Date.now();
// for (const oldEntry of listOld) {
//   // everything AFTER the first “#” in the old record
//   const fragment = oldEntry.slice(oldEntry.indexOf('#') + 1);
//     console.log(count++);
//   // does any new record (that starts with "0#") contain that fragment?
//   const hasTwinInNew = listNew.some(
//     newEntry => newEntry.startsWith('0#') && newEntry.includes(fragment)
//   );

//   if (hasTwinInNew) {
//     // rule: push the literal prefix "45734#" once for every match found
//     output.push(oldEntry.slice(0,oldEntry.indexOf('#')));
//   }
// }
// console.log(Date.now() - timeStart);
// console.log(new Set(output));       
// console.log(output.length);  

/**********************************************************************\
  Fast matcher for ≈100 k × 100 k lists
  –  No external deps
  –  One pass to build an index   ( O(M) )
  –  One pass to query the index  ( O(N·k) ,  k ≪ M )
\**********************************************************************/

const FIELDS_FOR_KEY = 10;      // tune (5‑8 is usually enough)
const SEP            = '#';
const ZERO_PREFIX    = '0#';
const OLD_PREFIX_LEN = 6;      // length of "45734#"

/* -----------------------------------------------------------
 * 1.  Build an index:  Map<bucketKey ,   string[] >
 * --------------------------------------------------------- */
function buildIndex(listNew) {
  const index = new Map();

  for (const rec of listNew) {
    if (!rec.startsWith(ZERO_PREFIX)) continue;          // we care only about 0‑records

    const key = getKey(rec, /*skipFirstToken=*/1);       // drop the leading “0”
    let bucket = index.get(key);
    if (!bucket) { bucket = []; index.set(key, bucket); }
    bucket.push(rec);
  }
  return index;
}

/* -----------------------------------------------------------
 * 2.  Look up every old record in its own bucket
 * --------------------------------------------------------- */
function matchOldWithNew(listOld, index) {
  const out   = [];
  const pref  = OLD_PREFIX_LEN;                // 45734#
  const seen  = new Set();                     // avoid pushing duplicate prefixes

  for (const oldRec of listOld) {
    const key      = getKey(oldRec, /*skipFirstToken=*/1);   // skip 45734
    const bucket   = index.get(key);
    if (!bucket) continue;                     // no chance to match

    const fragment = oldRec.slice(pref);       // everything after 45734#
    if (bucket.some(r => r.includes(fragment))) {
      const prefix = oldRec.slice(0, pref);    // "45734#"
      if (!seen.has(prefix)) {
        out.push(prefix);
        seen.add(prefix);
      }
    }
  }
  return out;
}

/* -----------------------------------------------------------
 * Small helper:  first FIELDS_FOR_KEY stable fields
 *   str  – full record
 *   skip – how many tokens to ignore at the beginning
 * --------------------------------------------------------- */
function getKey(str, skip) {
  // split just enough – no huge intermediate arrays
  const parts = str.split(SEP, FIELDS_FOR_KEY + skip + 1);
  return parts.slice(skip, skip + FIELDS_FOR_KEY).join(SEP);
}

/* -----------------------------------------------------------
 * 3.  Glue it together and time it
 * --------------------------------------------------------- */
function findPrefixes(listOld, listNew) {
  const t0     = Date.now();
  const index  = buildIndex(listNew);
  const output = matchOldWithNew(listOld, index);
  console.log(`time: ${Date.now() - t0} ms,  matches: ${output.length}`);
  return output;        //   ["45734#", "45734#", …]
}

/* -------------------   call it   -------------------------- */
let startTime = Date.now()
const prefixes = findPrefixes(listOld, listNew);
console.log(prefixes);
console.log(Date.now() - startTime);

