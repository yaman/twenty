// Simulates message sync batch operations via GraphQL mutations

import http from 'k6/http';
import { check, sleep } from 'k6';
import { Rate, Counter } from 'k6/metrics';
import { BASE_URL, headers, graphql } from './lib/config.js';

const errorRate = new Rate('sync_errors');
const messagesCreated = new Counter('messages_created');

export const options = {
  stages: [
    { duration: '30s', target: 5 },
    { duration: '3m', target: 20 },
    { duration: '1m', target: 0 },
  ],
  thresholds: {
    http_req_duration: ['p(95)<5000'],
    sync_errors: ['rate<0.10'],
  },
};

export default function () {
  const body = graphql(`
    mutation CreateMessage($input: MessageCreateInput!) {
      createMessage(data: $input) {
        id
      }
    }
  `, {
    input: {
      subject: `Load test message ${Date.now()}`,
      body: 'This is a load test message for sync simulation.',
      direction: 'INCOMING',
    }
  });

  const res = http.post(`${BASE_URL}/api`, body, { headers });

  const success = check(res, {
    'status is 200': (r) => r.status === 200,
  });

  if (success) messagesCreated.add(1);
  errorRate.add(!success);
  sleep(0.5);
}
