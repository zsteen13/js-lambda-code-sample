/* Amplify Params - DO NOT EDIT
    API_WATCHEXCHANGER_GRAPHQLAPIENDPOINTOUTPUT
    API_WATCHEXCHANGER_GRAPHQLAPIIDOUTPUT
    API_WATCHEXCHANGER_GRAPHQLAPIKEYOUTPUT
    AUTH_WATCHEXCHANGER_USERPOOLID
    ENV
    REGION
Amplify Params - DO NOT EDIT */

import crypto from '@aws-crypto/sha256-js';
import { defaultProvider } from '@aws-sdk/credential-provider-node';
import { SignatureV4 } from '@aws-sdk/signature-v4';
import { HttpRequest } from '@aws-sdk/protocol-http';
import fetch, { Request } from 'node-fetch';
import AWS from 'aws-sdk';

const GRAPHQL_ENDPOINT = process.env.API_WATCHEXCHANGER_GRAPHQLAPIENDPOINTOUTPUT;
const AWS_REGION = process.env.AWS_REGION || 'us-east-1';
const { Sha256 } = crypto;

const listQuery = (nextToken) => /* GraphQL */ `
  query listQuery($nextToken: String) {
    listUserInfos(filter: {collector: {eq: false}}, nextToken: $nextToken) {
      items {
        email,
        Subscriptions {
          items {
            ttl
          }
        }
      },
      nextToken
    }
  }
`;

const getSignedRequest = async (signer, query, variables) => {
  const endpoint = new URL(GRAPHQL_ENDPOINT);

  const requestToBeSigned = new HttpRequest({
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      host: endpoint.host
    },
    hostname: endpoint.host,
    body: JSON.stringify({ query, variables }),
    path: endpoint.pathname
  }); 

  return new Request(endpoint, await signer.sign(requestToBeSigned));
};

async function sendMail(email, subject, data, ses) {
  const emailParams = {
    Destination: {
      ToAddresses: [email],
    },
    Message: {
      Body: {
        Text: { Data: data },
      },
      Subject: { Data: subject },
    },
    Source: "Watch Exchanger <notifications@watchexchanger.com>",
  };

  try {
    await ses.sendEmail(emailParams).promise();
    console.log(`Email sent to: ${email}`);
    return { statusCode: 200 };
  } catch (error) {
    console.error(`Failed to send email to: ${email}, Error: ${error}`);
    return { statusCode: 400, reason: error.message };
  }
}

function isTomorrow(unixTimestamp) {
  const currentTime = Math.floor(Date.now() / 1000);
  const secondsInDay = 24 * 60 * 60;
  return unixTimestamp - currentTime < secondsInDay;
}

async function fetchUserInfos(signer, nextToken = null) {
  try {
    const request = await getSignedRequest(signer, listQuery, { nextToken });
    const response = await fetch(request);
    const body = await response.json();
    return body?.data?.listUserInfos;
  } catch (error) {
    console.error(`Error fetching user infos: ${error}`);
    throw error;
  }
}

const BATCH_SIZE = 100;

async function processEmailBatch(emails, subject, data, ses) {
  const sendPromises = emails.map(email => sendMail(email, subject, data, ses));
  await Promise.all(sendPromises);
}

export const handler = async (event) => {
  console.log(`EVENT: ${JSON.stringify(event)}`);

  const signer = new SignatureV4({
    credentials: defaultProvider(),
    region: AWS_REGION,
    service: 'appsync',
    sha256: Sha256
  });

  const userInfos = [];
  let nextToken = null;
  do {
    const listUserInfosResult = await fetchUserInfos(signer, nextToken);
    if (!listUserInfosResult) break;
    listUserInfosResult.items.forEach(item => userInfos.push(item));
    nextToken = listUserInfosResult.nextToken;
  } while (nextToken != null);

  const notifyList = new Set();
  userInfos.forEach(userInfo => {
    userInfo?.Subscriptions?.items?.forEach(subscription => {
      if (isTomorrow(subscription?.ttl)) {
        notifyList.add(userInfo?.email);
      }
    });
  });

  const ses = new AWS.SES({ region: AWS_REGION });

  if (notifyList.size > 0) {
    const emailArray = Array.from(notifyList);
    for (let i = 0; i < emailArray.length; i += BATCH_SIZE) {
      const batch = emailArray.slice(i, i + BATCH_SIZE);
      await processEmailBatch(batch, subject, data, ses);
    }
  }

  return { statusCode: 204 };
};

