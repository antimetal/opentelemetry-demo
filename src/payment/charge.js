// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0
const { context, propagation, trace, metrics } = require('@opentelemetry/api');
const cardValidator = require('simple-card-validator');
const { v4: uuidv4 } = require('uuid');

const { OpenFeature } = require('@openfeature/server-sdk');
const { FlagdProvider } = require('@openfeature/flagd-provider');
const flagProvider = new FlagdProvider();

const logger = require('./logger');
const tracer = trace.getTracer('payment');
const meter = metrics.getMeter('payment');
const transactionsCounter = meter.createCounter('app.payment.transactions');
const paymentFailureFlagCounter = meter.createCounter('app.payment.failure_flag_evaluations', {
  description: 'Count of requests where paymentFailure flag was active (value > 0)',
});
const paymentFailureInducedCounter = meter.createCounter('app.payment.failure_flag_induced', {
  description: 'Count of payment failures induced by the paymentFailure feature flag',
});

const LOYALTY_LEVEL = ['platinum', 'gold', 'silver', 'bronze'];

/** Return random element from given array */
function random(arr) {
  const index = Math.floor(Math.random() * arr.length);
  return arr[index];
}

module.exports.charge = async request => {
  const span = tracer.startSpan('charge');

  await OpenFeature.setProviderAndWait(flagProvider);

  const numberVariant =  await OpenFeature.getClient().getNumberValue("paymentFailure", 0);

  if (numberVariant > 0) {
    logger.warn({ flag: 'paymentFailure', failureRate: numberVariant }, 'paymentFailure feature flag is active — injecting failures');
    paymentFailureFlagCounter.add(1, { 'app.payment.flag_active': true });

    const allowFailureInjection = ['test', 'development', 'staging'].includes(
      (process.env.ENVIRONMENT || process.env.NODE_ENV || '').toLowerCase()
    );

    if (!allowFailureInjection) {
      logger.error({ flag: 'paymentFailure', failureRate: numberVariant }, 'paymentFailure flag is active in a non-test environment — ignoring to protect production traffic');
    } else {
      // n% chance to fail with app.loyalty.level=gold
      if (Math.random() < numberVariant) {
        span.setAttributes({'app.loyalty.level': 'gold' });
        paymentFailureInducedCounter.add(1, { 'app.payment.failure_reason': 'feature_flag' });
        span.end();

        throw new Error('Payment request failed. Invalid token. app.loyalty.level=gold');
      }
    }
  }

  const {
    creditCardNumber: number,
    creditCardExpirationYear: year,
    creditCardExpirationMonth: month
  } = request.creditCard;
  const currentMonth = new Date().getMonth() + 1;
  const currentYear = new Date().getFullYear();
  const lastFourDigits = number.substr(-4);
  const transactionId = uuidv4();

  const card = cardValidator(number);
  const { card_type: cardType, valid } = card.getCardDetails();

  const loyalty_level = random(LOYALTY_LEVEL);

  span.setAttributes({
    'app.payment.card_type': cardType,
    'app.payment.card_valid': valid,
    'app.loyalty.level': loyalty_level
  });

  if (!valid) {
    throw new Error('Credit card info is invalid.');
  }

  if (!['visa', 'mastercard'].includes(cardType)) {
    throw new Error(`Sorry, we cannot process ${cardType} credit cards. Only VISA or MasterCard is accepted.`);
  }

  if ((currentYear * 12 + currentMonth) > (year * 12 + month)) {
    throw new Error(`The credit card (ending ${lastFourDigits}) expired on ${month}/${year}.`);
  }

  // Check baggage for synthetic_request=true, and add charged attribute accordingly
  const baggage = propagation.getBaggage(context.active());
  if (baggage && baggage.getEntry('synthetic_request') && baggage.getEntry('synthetic_request').value === 'true') {
    span.setAttribute('app.payment.charged', false);
  } else {
    span.setAttribute('app.payment.charged', true);
  }

  const { units, nanos, currencyCode } = request.amount;
  logger.info({ transactionId, cardType, lastFourDigits, amount: { units, nanos, currencyCode }, loyalty_level }, 'Transaction complete.');
  transactionsCounter.add(1, { 'app.payment.currency': currencyCode });
  span.end();

  return { transactionId };
};
