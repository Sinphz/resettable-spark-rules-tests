'use strict';
// Real SDK + Firestore Rules tests, ONLY on a loopback demo-project emulator.
// No imports of app startup, production config, Admin SDK or cloud credentials.
// Nothing in this file deploys rules. --check-config performs NO SDK/network I/O.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const PROJECT = 'demo-resettable-spark';
const HOST = '127.0.0.1';
const PORT = 8080;
const PROTOCOL = 'resettable-spark-v1';
const OWNER = 'rules_owner_A';
const OTHER = 'rules_owner_B';
const CONFIG_PATH = path.resolve(__dirname, '../../firebase.json');
const RULES_PATH = path.resolve(__dirname, '../../firestore-spark-prototype.rules');
function isolationGuard() {
  assert.equal(process.env.RESETTABLE_SPARK_EMULATOR_TEST, '1', 'Explicit emulator-test opt-in is required');
  assert.equal(process.env.FIRESTORE_EMULATOR_HOST, `${HOST}:${PORT}`, 'Only 127.0.0.1:8080 is allowed; no emulator fallback');
  assert.equal(process.env.FIREBASE_EMULATOR_HUB, '127.0.0.1:4400', 'Only the local emulator hub is allowed');
  assert.equal(process.env.GCLOUD_PROJECT, PROJECT, 'Only the fictitious demo project is allowed');
  assert.equal(process.env.GOOGLE_CLOUD_PROJECT, PROJECT, 'Only the fictitious demo project is allowed');
  for (const key of ['FIREBASE_TOKEN', 'FIREBASE_CONFIG', 'GOOGLE_APPLICATION_CREDENTIALS',
    'GOOGLE_OAUTH_ACCESS_TOKEN', 'CLOUDSDK_AUTH_ACCESS_TOKEN', 'CLOUDSDK_AUTH_CREDENTIAL_FILE_OVERRIDE',
    'FIREBASE_AUTH_EMULATOR_HOST', 'FIREBASE_DATABASE_EMULATOR_HOST', 'FIREBASE_STORAGE_EMULATOR_HOST', 'WEB_HOST']) {
    assert(!process.env[key], `Unexpected credentials/configuration in ${key}; refusing to run`);
  }
  for (const key of ['FIREBASE_PROJECT', 'FIREBASE_PROJECT_ID', 'CLOUDSDK_CORE_PROJECT']) {
    assert(!process.env[key] || process.env[key] === PROJECT, `Unexpected project in ${key}`);
  }
  assert(!process.env.FIRESTORE_EMULATOR_HOST_PATH || process.env.FIRESTORE_EMULATOR_HOST_PATH === `${HOST}:${PORT}`);
  const config = JSON.parse(fs.readFileSync(CONFIG_PATH, 'utf8'));
  assert.deepEqual(config, {
    firestore: { rules: 'firestore-spark-prototype.rules' },
    emulators: { firestore: { host: HOST, port: PORT }, hub: { host: HOST, port: 4400 },
      logging: { host: HOST, port: 4500 }, ui: { enabled: false }, singleProjectMode: true }
  }, 'The isolated emulator configuration must not enable other services or hosts');
  assert(fs.lstatSync(RULES_PATH).isFile(), 'The existing draft Rules must be a regular file');
  const rules = fs.readFileSync(RULES_PATH, 'utf8');
  assert(rules.trim().length > 0 && rules.includes("rules_version = '2'"), 'Missing or unexpected Rules source');
  return rules;
}
// Guard before loading ANY dependency. Missing host/opt-in never starts a client.
const rules = isolationGuard();
if (process.argv.includes('--check-config')) {
  console.log(`Isolation preflight passed: ${PROJECT}, ${HOST}:${PORT}; no SDK loaded or network request made.`);
  process.exit(0);
}
const { test, before, beforeEach, after } = require('node:test');
const { initializeTestEnvironment, assertSucceeds, assertFails } = require('@firebase/rules-unit-testing');
const { doc, collection, getDocs, getDocFromServer, writeBatch, runTransaction,
  serverTimestamp, setDoc, updateDoc, deleteDoc } = require('firebase/firestore');
let env;
before(async () => {
  isolationGuard();
  // Explicit loopback endpoints: no automatic emulator discovery or real Auth.
  // Loading/compiling Rules here is emulator-only; errors fail the whole suite.
  env = await initializeTestEnvironment({ projectId: PROJECT,
    hub: { host: HOST, port: 4400 }, firestore: { host: HOST, port: PORT, rules } });
}, { timeout: 60000 });
beforeEach(async () => { isolationGuard(); await env.clearFirestore(); }, { timeout: 15000 });
after(async () => { if (env) await env.cleanup(); }, { timeout: 30000 });
const ruleTest = (name, fn) => test(name, { timeout: 20000 }, fn);
function db(uid = OWNER, provider = 'google.com') {
  isolationGuard();
  return env.authenticatedContext(uid, { firebase: { sign_in_provider: provider } }).firestore();
}
function paths(uid, epoch, sub) {
  const head = `users/${uid}/syncSparkV1/head`, cursor = `${head}/epochs/${epoch}`;
  return { head, cursor, commit: `${cursor}/commits/${sub}` };
}
function submission(overrides = {}) {
  // JSON is opaque to these Rules. App-level semantic validation is separate.
  const payloadJson = JSON.stringify({ progress: {
    'resettable-checklist-v1': {}, 'resettable-custom-spaces-v1': [],
    'resettable-space-goals-v1': {}, 'resettable-reset-session-v1': null,
    'resettable-reset-schedule-v1': [] }, session: null, usedSessionIds: [] });
  return Object.freeze({ protocol: PROTOCOL, uid: OWNER, epochId: 'epoch_A', submissionId: 'submission_1',
    operationId: 'operation_1', sequence: 1, expectedCloudRevision: 0, expectedBaselineId: null,
    localRevision: 2, payloadJson, ...overrides });
}
function documents(s, oldHead = null, oldCursor = null) {
  const c = { ...s, cloudRevision: s.expectedCloudRevision + 1,
    baselineId: `${s.epochId}:${s.submissionId}`, committedAt: serverTimestamp() };
  const h = { protocol: PROTOCOL, uid: s.uid, revision: c.cloudRevision, baselineId: c.baselineId,
    epochId: s.epochId, submissionId: s.submissionId, sequence: s.sequence,
    payloadJson: s.payloadJson, epochCount: (oldHead?.epochCount ?? 0) + (oldCursor === null ? 1 : 0), committedAt: serverTimestamp() };
  const cursor = { protocol: PROTOCOL, uid: s.uid, epochId: s.epochId, sequence: s.sequence,
    operationIds: [...(oldCursor?.operationIds ?? []), s.operationId], lastSubmissionId: s.submissionId, cloudRevision: c.cloudRevision };
  const p = paths(s.uid, s.epochId, s.submissionId);
  return [{ path: p.head, data: h }, { path: p.cursor, data: cursor }, { path: p.commit, data: c }];
}
async function write(db, writes) {
  const batch = writeBatch(db); for (const w of writes) batch.set(doc(db, w.path), w.data); return batch.commit();
}
async function serverDoc(db, p) {
  const snap = await getDocFromServer(doc(db, p));
  assert.equal(snap.metadata.fromCache, false); assert.equal(snap.metadata.hasPendingWrites, false);
  return snap;
}
function exactCommit(s, c) {
  assert.deepEqual(Object.fromEntries(Object.keys(s).map(k => [k, c[k]])), s, 'Exact retry content mismatch');
  assert.equal(c.cloudRevision, s.expectedCloudRevision + 1);
  assert.equal(c.baselineId, `${s.epochId}:${s.submissionId}`);
  assert(c.committedAt && typeof c.committedAt.toMillis === 'function');
}
async function accept(db, s) {
  // Frozen parent checks are deliberately inside EVERY transaction invocation.
  // This is test-client code, not a production adapter. No local app mutations.
  const p = paths(s.uid, s.epochId, s.submissionId);
  return runTransaction(db, async tx => {
    const existing = await tx.get(doc(db, p.commit));
    if (existing.exists()) { exactCommit(s, existing.data()); return { duplicate: true }; }
    const head = await tx.get(doc(db, p.head)), cursor = await tx.get(doc(db, p.cursor));
    const h = head.exists() ? head.data() : null, c = cursor.exists() ? cursor.data() : null;
    assert.equal(h?.revision ?? 0, s.expectedCloudRevision, 'frozen-revision-conflict');
    assert.equal(h?.baselineId ?? null, s.expectedBaselineId, 'frozen-baseline-conflict');
    for (const w of documents(s, h, c)) tx.set(doc(db, w.path), w.data);
    return { duplicate: false };
  });
}
async function saved(db, s) {
  const p = paths(s.uid, s.epochId, s.submissionId);
  return { head: (await serverDoc(db, p.head)).data(), cursor: (await serverDoc(db, p.cursor)).data(), commit: (await serverDoc(db, p.commit)).data() };
}
async function initial(client = db()) {
  const s = submission(); await assertSucceeds(accept(client, s)); return { client, s, data: await saved(client, s) };
}

ruleTest('valid initial atomic commit creates all three matching documents', async () => {
  const { s, data } = await initial(); exactCommit(s, data.commit);
  assert.equal(data.head.revision, 1); assert.equal(data.head.epochCount, 1);
  assert.deepEqual(data.cursor.operationIds, [s.operationId]);
  assert(data.head.committedAt.isEqual(data.commit.committedAt));
});
ruleTest('password synthetic authentication is also permitted', async () => { await initial(db(OWNER, 'password')); });
ruleTest('owner can get its head, cursor and immutable commit; list remains denied', async () => {
  const { client, s } = await initial(); const p = paths(OWNER, s.epochId, s.submissionId);
  for (const key of Object.values(p)) assert((await serverDoc(client, key)).exists());
  await assertFails(getDocs(collection(client, `${p.head}/epochs`)));
  await assertFails(getDocs(collection(client, `${p.cursor}/commits`)));
});
ruleTest('another account cannot read any owner sync document', async () => {
  const { s } = await initial(), attacker = db(OTHER), p = paths(OWNER, s.epochId, s.submissionId);
  for (const key of Object.values(p)) await assertFails(getDocFromServer(doc(attacker, key)));
});
ruleTest('another account cannot create or overwrite owner documents', async () => {
  const attacker = db(OTHER); await assertFails(write(attacker, documents(submission())));
  const { s, data } = await initial(); const next = submission({ submissionId: 'forged', operationId: 'forged', sequence: 2,
    expectedCloudRevision: 1, expectedBaselineId: data.head.baselineId });
  await assertFails(write(attacker, documents(next, data.head, data.cursor)));
  const own = submission({ uid: OTHER }); await assertSucceeds(accept(attacker, own));
  assert.equal((await saved(db(), s)).head.revision, 1);
});
ruleTest('unauthenticated and anonymous contexts cannot read or write', async () => {
  const { s } = await initial(); const p = paths(OWNER, s.epochId, s.submissionId);
  for (const client of [env.unauthenticatedContext().firestore(), db(OWNER, 'anonymous')]) {
    await assertFails(getDocFromServer(doc(client, p.head)));
    await assertFails(write(client, documents(submission({ submissionId: 'blocked' }))));
  }
});
for (const omitted of [0, 1, 2]) ruleTest(`initial atomic write rejects omitted document ${omitted}`, async () => {
  const client = db(), s = submission(); await assertFails(write(client, documents(s).filter((_, i) => i !== omitted)));
  for (const p of Object.values(paths(OWNER, s.epochId, s.submissionId))) assert.equal((await serverDoc(client, p)).exists(), false);
});
for (const only of [0, 1, 2]) ruleTest(`an isolated write of document ${only} is denied`, async () => {
  await assertFails(write(db(), [documents(submission())[only]]));
});
ruleTest('partial updates cannot advance an existing head without its cursor and commit', async () => {
  const { client, data } = await initial(); const s = submission({ submissionId: 'next', operationId: 'next', sequence: 2,
    expectedCloudRevision: 1, expectedBaselineId: data.head.baselineId });
  const ws = documents(s, data.head, data.cursor);
  await assertFails(write(client, ws.slice(0, 2)));
  assert.equal((await serverDoc(client, ws[0].path)).data().revision, 1);
  assert.equal((await serverDoc(client, ws[1].path)).data().sequence, 1);
});
ruleTest('valid returning-epoch commit increments account revision and epoch cursor', async () => {
  const { client, data } = await initial(); const s = submission({ submissionId: 'next', operationId: 'next', sequence: 2,
    expectedCloudRevision: 1, expectedBaselineId: data.head.baselineId });
  await assertSucceeds(accept(client, s)); const next = await saved(client, s);
  assert.equal(next.head.revision, 2); assert.equal(next.head.epochCount, 1); assert.equal(next.cursor.sequence, 2);
});
ruleTest('second epoch starts its own sequence while sharing the account revision', async () => {
  const { client, data } = await initial(); const s = submission({ epochId: 'epoch_B', submissionId: 'B_first',
    expectedCloudRevision: 1, expectedBaselineId: data.head.baselineId });
  await assertSucceeds(accept(client, s)); const next = await saved(client, s);
  assert.equal(next.head.revision, 2); assert.equal(next.head.epochCount, 2); assert.equal(next.cursor.sequence, 1);
  assert.equal((await serverDoc(client, paths(OWNER, 'epoch_A', 'submission_1').cursor)).data().sequence, 1);
});
ruleTest('stale raw writes are denied by Rules independently of client conflict checks', async () => {
  const { client, data } = await initial(); const stale = submission({ epochId: 'epoch_B', submissionId: 'stale', operationId: 'stale' });
  await assertFails(write(client, documents(stale, data.head)));
  assert.equal((await serverDoc(client, paths(OWNER, 'epoch_B', 'stale').commit)).exists(), false);
  assert.equal((await saved(client, submission())).head.revision, 1);
});
ruleTest('frozen client transaction does not silently rebase its parent revision', async () => {
  const { client } = await initial(); const stale = submission({ epochId: 'epoch_B', submissionId: 'stale' });
  await assert.rejects(accept(client, stale), /frozen-revision-conflict/);
});
ruleTest('competing atomic writes from the same baseline cannot both succeed', async () => {
  const client = db(), a = submission(), b = submission({ epochId: 'epoch_B', submissionId: 'B', operationId: 'B' });
  const results = await Promise.allSettled([write(client, documents(a)), write(db(), documents(b))]);
  assert.equal(results.filter(r => r.status === 'fulfilled').length, 1);
  const failure = results.find(r => r.status === 'rejected'); assert.equal(failure.reason.code, 'permission-denied');
  assert.equal((await serverDoc(client, paths(OWNER, 'epoch_A', 'submission_1').head)).data().revision, 1);
});
ruleTest('immutable commits cannot be updated, overwritten or deleted', async () => {
  const { client, s } = await initial(); const ref = doc(client, paths(OWNER, s.epochId, s.submissionId).commit);
  await assertFails(updateDoc(ref, { payloadJson: 'changed' }));
  await assertFails(setDoc(ref, { invalid: true })); await assertFails(deleteDoc(ref));
  exactCommit(s, (await serverDoc(client, ref.path)).data());
});
ruleTest('head and cursor deletion are denied, preserving replay history', async () => {
  const { client, s } = await initial(), p = paths(OWNER, s.epochId, s.submissionId);
  await assertFails(deleteDoc(doc(client, p.head))); await assertFails(deleteDoc(doc(client, p.cursor)));
});
ruleTest('exact retry recovers the original receipt without a second revision', async () => {
  const { client, s, data } = await initial(); assert.deepEqual(await accept(client, s), { duplicate: true });
  const next = await saved(client, s); assert.equal(next.head.revision, 1);
  assert(next.commit.committedAt.isEqual(data.commit.committedAt)); exactCommit(s, next.commit);
});
ruleTest('lost reply recovers original immutable commit after another epoch advances the head', async () => {
  const client = db(), original = submission();
  // Acceptance is durable, but simulate the client not receiving its response.
  await assertSucceeds(accept(client, original));
  const first = await saved(client, original);
  const b = submission({ epochId: 'epoch_B', submissionId: 'B_first',
    expectedCloudRevision: 1, expectedBaselineId: first.head.baselineId });
  await assertSucceeds(accept(db(), b));
  assert.deepEqual(await accept(db(), original), { duplicate: true });
  const recovered = await saved(client, original); assert.equal(recovered.head.revision, 2);
  exactCommit(original, recovered.commit); assert(recovered.commit.committedAt.isEqual(first.commit.committedAt));
});
ruleTest('changed retry contents cannot replace an existing submission', async () => {
  const { client, s, data } = await initial();
  await assert.rejects(accept(client, submission({ ...s, payloadJson: 'different' })), /Exact retry content mismatch/);
  await assertFails(write(client, documents(submission({ ...s, payloadJson: 'different' }), data.head, data.cursor)));
  exactCommit(s, (await saved(client, s)).commit);
});
ruleTest('identical submission and operation IDs on different epochs remain separate', async () => {
  const { client, s, data } = await initial(); const b = submission({ epochId: 'epoch_B',
    expectedCloudRevision: 1, expectedBaselineId: data.head.baselineId });
  await assertSucceeds(accept(client, b)); assert.deepEqual(await accept(client, s), { duplicate: true });
  assert((await serverDoc(client, paths(OWNER, b.epochId, b.submissionId).commit)).exists());
});
ruleTest('reused operation IDs in one epoch and sequence gaps are denied', async () => {
  const { client, data } = await initial();
  for (const overrides of [{ operationId: 'operation_1', sequence: 2 }, { operationId: 'new', sequence: 3 }]) {
    const s = submission({ submissionId: 'bad', expectedCloudRevision: 1, expectedBaselineId: data.head.baselineId, ...overrides });
    await assertFails(write(client, documents(s, data.head, data.cursor)));
  }
});
ruleTest('allowed field sets and matching payloads are enforced across documents', async () => {
  for (const mutate of [ws => { ws[2].data.extra = true; }, ws => { ws[0].data.payloadJson = 'different'; },
    ws => { ws[1].data.lastSubmissionId = 'different'; }, ws => { ws[2].data.uid = OTHER; }]) {
    const ws = documents(submission()); mutate(ws); await assertFails(write(db(), ws));
  }
});
ruleTest('a client-supplied old timestamp cannot replace request.time', async () => {
  const { Timestamp } = require('firebase/firestore'); const ws = documents(submission());
  ws[0].data.committedAt = Timestamp.fromMillis(1); ws[2].data.committedAt = Timestamp.fromMillis(1);
  await assertFails(write(db(), ws));
});
ruleTest('payload bound accepts 32 KiB ASCII and rejects overflow/non-ASCII', async () => {
  const client = db(); await assertSucceeds(accept(client, submission({ payloadJson: 'x'.repeat(32768) })));
  for (const payloadJson of ['x'.repeat(32769), 'non-ASCII café']) {
    const s = submission({ uid: OTHER, payloadJson }); await assertFails(write(db(OTHER), documents(s)));
  }
});
ruleTest('eight epoch cursors are allowed and a ninth is rejected without pruning', async () => {
  const client = db(); let head = null;
  for (let i = 1; i <= 8; i++) {
    const s = submission({ epochId: `epoch_${i}`, submissionId: `s_${i}`,
      expectedCloudRevision: head?.revision ?? 0, expectedBaselineId: head?.baselineId ?? null });
    await assertSucceeds(accept(client, s)); head = (await saved(client, s)).head;
  }
  const s = submission({ epochId: 'epoch_9', submissionId: 's_9', expectedCloudRevision: head.revision, expectedBaselineId: head.baselineId });
  await assertFails(write(client, documents(s, head)));
  assert.equal((await serverDoc(client, paths(OWNER, 'epoch_1', 's_1').head)).data().epochCount, 8);
  assert((await serverDoc(client, paths(OWNER, 'epoch_1', 's_1').commit)).exists());
});
ruleTest('one batch cannot create two accepted commits sharing one resulting head', async () => {
  const client = db(), first = submission(), other = submission({ epochId: 'epoch_B', submissionId: 'B' });
  const a = documents(first), b = documents(other), batch = writeBatch(client);
  for (const w of [...a, b[1], b[2]]) batch.set(doc(client, w.path), w.data);
  await assertFails(batch.commit()); assert.equal((await serverDoc(client, a[0].path)).exists(), false);
});
