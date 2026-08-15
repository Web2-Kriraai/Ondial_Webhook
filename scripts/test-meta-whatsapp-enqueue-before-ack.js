/**
 * Contract: Meta POST must await BullMQ enqueue before returning 200.
 * A Redis blip that fails enqueue must surface as 503 so Meta retries.
 *
 *   node scripts/test-meta-whatsapp-enqueue-before-ack.js
 */
const assert = require("assert");
const fs = require("fs");
const path = require("path");

const indexSrc = fs.readFileSync(path.join(__dirname, "..", "index.js"), "utf8");
const queueSrc = fs.readFileSync(path.join(__dirname, "..", "metaInboundQueue.js"), "utf8");

const postHandlerStart = indexSrc.indexOf('app.post("/api/webhook/whatsapp"');
assert.ok(postHandlerStart >= 0, "Meta POST route missing");
const nextRoute = indexSrc.indexOf("app.get(", postHandlerStart + 1);
const postHandler = indexSrc.slice(postHandlerStart, nextRoute > postHandlerStart ? nextRoute : undefined);

const enqueueIdx = postHandler.indexOf("await enqueueMetaWhatsappInbound");
const ackIdx = postHandler.indexOf('res.status(200).json({ received: true })');
const failIdx = postHandler.indexOf('status(503).json({ error: "Queue unavailable"');

assert.ok(enqueueIdx >= 0, "Meta POST must await enqueueMetaWhatsappInbound");
assert.ok(ackIdx >= 0, "Meta POST must ACK with 200 after enqueue");
assert.ok(failIdx >= 0, "Meta POST must return 503 when enqueue fails");
assert.ok(
    enqueueIdx < failIdx && failIdx < ackIdx,
    "Ordering must be: await enqueue → catch 503 → then 200 ACK"
);
assert.ok(
    !/res\.status\(200\).*enqueueMetaWhatsappInbound/s.test(postHandler),
    "Must not ACK before enqueue"
);

assert.ok(queueSrc.includes("await q.add("), "enqueue must await BullMQ Queue.add (Redis command reply)");
assert.ok(queueSrc.includes("throw err"), "enqueue failures must propagate to the route");

console.log("OK: Meta WhatsApp enqueue-before-ACK contract holds");
