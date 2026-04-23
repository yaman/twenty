// Tests GraphQL query throughput for core operations
//
// Usage:
//   k6 run infra/validation/k6/graphql-throughput.js
//   k6 run --out experimental-prometheus-rw infra/validation/k6/graphql-throughput.js

import http from 'k6/http';
import { check, sleep } from 'k6';
import { Rate, Trend } from 'k6/metrics';
import { BASE_URL, headers, graphql } from './lib/config.js';

const errorRate = new Rate('errors');
const queryDuration = new Trend('query_duration', true);

export const options = {
  stages: [
    { duration: '30s', target: 10 },
    { duration: '2m', target: 50 },
    { duration: '1m', target: 100 },
    { duration: '2m', target: 100 },
    { duration: '30s', target: 0 },
  ],
  thresholds: {
    http_req_duration: ['p(95)<2000'],
    errors: ['rate<0.05'],
  },
};

const QUERIES = {
  companyList: graphql(`
    query Companies($first: Int) {
      companies(first: $first) {
        edges {
          node { id name }
        }
      }
    }
  `, { first: 20 }),

  personList: graphql(`
    query People($first: Int) {
      people(first: $first) {
        edges {
          node { id name { firstName lastName } }
        }
      }
    }
  `, { first: 20 }),

  opportunityPipeline: graphql(`
    query Opportunities($first: Int) {
      opportunities(first: $first) {
        edges {
          node { id name stage amount { amountMicros currencyCode } }
        }
      }
    }
  `, { first: 50 }),
};

export default function () {
  const queryNames = Object.keys(QUERIES);
  const queryName = queryNames[Math.floor(Math.random() * queryNames.length)];
  const body = QUERIES[queryName];

  const start = Date.now();
  const res = http.post(`${BASE_URL}/graphql`, body, { headers });
  queryDuration.add(Date.now() - start);

  const success = check(res, {
    'status is 200': (r) => r.status === 200,
    'no GraphQL errors': (r) => {
      const json = r.json();
      return !json.errors || json.errors.length === 0;
    },
  });

  errorRate.add(!success);
  sleep(0.1);
}
