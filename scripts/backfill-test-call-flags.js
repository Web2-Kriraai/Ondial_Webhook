/**
 * One-off: set isTestCall on CallLogs where events explicitly show is_test_call/isTestCall.
 * Usage: MONGODB_URI=... node scripts/backfill-test-call-flags.js [campaignId]
 */
const { MongoClient } = require('mongodb');

const uri = process.env.MONGODB_URI;
const campaignId = process.argv[2] || null;

function inferFromDoc(log) {
  if (log?.isTestCall === true) return true;
  const events = Array.isArray(log?.call_data?.events) ? log.call_data.events : [];
  for (const event of events) {
    const call = event?.data?._raw?.call || event?.data?.call;
    if (!call) continue;
    const cp = call.customParameters && typeof call.customParameters === 'object' ? call.customParameters : null;
    if (cp?.is_test_call === true || cp?.is_test_call === 'true') return true;
    if (cp?.isTestCall === true || cp?.isTestCall === 'true') return true;
  }
  return false;
}

(async () => {
  const c = new MongoClient(uri);
  await c.connect();
  const col = c.db('ondial').collection('CallLogs');
  const query = campaignId ? { campaign_id: campaignId } : {};
  const cursor = col.find(query);
  let updated = 0;
  let scanned = 0;
  while (await cursor.hasNext()) {
    const log = await cursor.next();
    scanned += 1;
    if (!inferFromDoc(log)) continue;
    await col.updateOne({ _id: log._id }, { $set: { isTestCall: true, updatedAt: new Date() } });
    updated += 1;
    console.log('marked', log.call_id, log.campaign_id);
  }
  console.log({ scanned, updated });
  await c.close();
})();
