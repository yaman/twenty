// BullMQ + KeyDB compatibility validation
// Tests all patterns used by Twenty's 17 queues
//
// Usage:
//   REDIS_URL=redis://localhost:6379 node test.js
//   REDIS_URL=redis://twenty-keydb:6379 node test.js

const { Queue, Worker } = require('bullmq');
const Redis = require('ioredis');

const REDIS_URL = process.env.REDIS_URL || 'redis://localhost:6379';
const connection = { url: REDIS_URL };

const TWENTY_QUEUES = [
  'calendar-queue', 'email-queue', 'message-queue',
  'webhook-queue', 'workflow-queue', 'contact-queue',
  'billing-queue', 'connected-account-queue', 'cron-queue',
  'data-seed-demo-workspace-queue', 'record-crud-queue',
  'search-queue', 'timeline-queue', 'workspace-queue',
  'duplicate-queue', 'company-queue', 'field-mapping-queue'
];

let passed = 0;
let failed = 0;

async function assert(name, fn) {
  try {
    await fn();
    console.log(`  ✓ ${name}`);
    passed++;
  } catch (err) {
    console.log(`  ✗ ${name}: ${err.message}`);
    failed++;
  }
}

async function testQueueCreation() {
  console.log('\n1. Queue Creation (all 17 queues)');
  const queues = [];
  for (const name of TWENTY_QUEUES) {
    const q = new Queue(name, { connection });
    queues.push(q);
  }
  await assert('Create all 17 queues', async () => {
    if (queues.length !== 17) throw new Error(`Expected 17, got ${queues.length}`);
  });
  for (const q of queues) await q.close();
}

async function testJobProcessing() {
  console.log('\n2. Job Processing');
  const queue = new Queue('test-processing', { connection });
  let processed = false;

  const worker = new Worker('test-processing', async (job) => {
    if (job.data.key === 'test-value') processed = true;
    return { result: 'ok' };
  }, { connection });

  await queue.add('test-job', { key: 'test-value' });
  await new Promise(r => setTimeout(r, 2000));

  await assert('Process a job', async () => {
    if (!processed) throw new Error('Job was not processed');
  });

  await worker.close();
  await queue.close();
}

async function testJobPriority() {
  console.log('\n3. Job Priority (7 levels)');
  const queue = new Queue('test-priority', { connection });
  const order = [];

  const worker = new Worker('test-priority', async (job) => {
    order.push(job.data.priority);
    return {};
  }, { connection });

  await worker.pause();
  for (let p = 7; p >= 1; p--) {
    await queue.add('p-job', { priority: p }, { priority: p });
  }
  await worker.resume();
  await new Promise(r => setTimeout(r, 3000));

  await assert('Process jobs in priority order', async () => {
    if (order.length < 5) throw new Error(`Only processed ${order.length} of 7 jobs`);
    if (order[0] !== 1) throw new Error(`Expected priority 1 first, got ${order[0]}`);
  });

  await worker.close();
  await queue.close();
}

async function testJobScheduling() {
  console.log('\n4. Job Scheduling (upsertJobScheduler)');
  const queue = new Queue('test-scheduling', { connection });
  let scheduledRun = false;

  const worker = new Worker('test-scheduling', async () => {
    scheduledRun = true;
    return {};
  }, { connection });

  await queue.upsertJobScheduler('test-scheduler', { every: 1000 }, { data: {} });
  await new Promise(r => setTimeout(r, 3000));

  await assert('Scheduled job executes', async () => {
    if (!scheduledRun) throw new Error('Scheduled job did not execute');
  });

  await queue.removeJobScheduler('test-scheduler');
  await worker.close();
  await queue.close();
}

async function testRetry() {
  console.log('\n5. Job Retry');
  const queue = new Queue('test-retry', { connection });
  let attempts = 0;

  const worker = new Worker('test-retry', async () => {
    attempts++;
    if (attempts < 3) throw new Error('Simulated failure');
    return {};
  }, { connection });

  await queue.add('retry-job', {}, { attempts: 5, backoff: { type: 'fixed', delay: 500 } });
  await new Promise(r => setTimeout(r, 5000));

  await assert('Job retries and succeeds', async () => {
    if (attempts < 3) throw new Error(`Only ${attempts} attempts, expected >=3`);
  });

  await worker.close();
  await queue.close();
}

async function testPubSub() {
  console.log('\n6. Redis Pub/Sub');
  const sub = new Redis(REDIS_URL);
  const pub = new Redis(REDIS_URL);
  let received = false;

  await new Promise((resolve) => {
    sub.subscribe('test-channel', () => {
      pub.publish('test-channel', 'hello');
    });
    sub.on('message', (channel, message) => {
      if (channel === 'test-channel' && message === 'hello') {
        received = true;
        resolve();
      }
    });
    setTimeout(resolve, 3000);
  });

  await assert('Pub/sub message received', async () => {
    if (!received) throw new Error('Message not received');
  });

  await sub.quit();
  await pub.quit();
}

async function testCacheOps() {
  console.log('\n7. Cache Operations (get/set/mget/mset)');
  const redis = new Redis(REDIS_URL);

  await redis.set('test:key1', 'value1');
  const v1 = await redis.get('test:key1');
  await assert('SET/GET', async () => {
    if (v1 !== 'value1') throw new Error(`Expected 'value1', got '${v1}'`);
  });

  await redis.mset('test:mk1', 'mv1', 'test:mk2', 'mv2');
  const mv = await redis.mget('test:mk1', 'test:mk2');
  await assert('MSET/MGET', async () => {
    if (mv[0] !== 'mv1' || mv[1] !== 'mv2') throw new Error(`Unexpected: ${mv}`);
  });

  await redis.quit();
}

async function testSetOps() {
  console.log('\n8. SET Operations (SADD/SREM/SPOP/SMEMBERS)');
  const redis = new Redis(REDIS_URL);

  await redis.sadd('test:set', 'a', 'b', 'c');
  const members = await redis.smembers('test:set');
  await assert('SADD/SMEMBERS', async () => {
    if (members.length !== 3) throw new Error(`Expected 3, got ${members.length}`);
  });

  await redis.srem('test:set', 'b');
  const after = await redis.smembers('test:set');
  await assert('SREM', async () => {
    if (after.includes('b')) throw new Error('b should have been removed');
  });

  const popped = await redis.spop('test:set');
  await assert('SPOP', async () => {
    if (!['a', 'c'].includes(popped)) throw new Error(`Unexpected pop: ${popped}`);
  });

  await redis.quit();
}

async function cleanup() {
  const redis = new Redis(REDIS_URL);
  const keys = await redis.keys('test:*');
  if (keys.length > 0) await redis.del(...keys);
  for (const q of [...TWENTY_QUEUES, 'test-processing', 'test-priority', 'test-scheduling', 'test-retry']) {
    const bkeys = await redis.keys(`bull:${q}:*`);
    if (bkeys.length > 0) await redis.del(...bkeys);
  }
  await redis.quit();
}

async function main() {
  console.log(`BullMQ + KeyDB Compatibility Test`);
  console.log(`Target: ${REDIS_URL}`);

  try {
    await testQueueCreation();
    await testJobProcessing();
    await testJobPriority();
    await testJobScheduling();
    await testRetry();
    await testPubSub();
    await testCacheOps();
    await testSetOps();
  } finally {
    await cleanup();
  }

  console.log(`\n=== Results: ${passed} passed, ${failed} failed ===`);
  process.exit(failed > 0 ? 1 : 0);
}

main().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});
