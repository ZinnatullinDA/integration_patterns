import amqp from 'amqplib';
import { setTimeout as delay } from 'node:timers/promises';
import { writeFile, rm } from 'node:fs/promises';

const READY = '/tmp/ready';

class Runtime {
  constructor() {
    this.service = process.env.SERVICE_NAME || 'app';
    this.controller = new AbortController();
    this.jobs = new Set();
    this.tags = [];
    this.stopping = false;
    this.closing = false;
    this.fault = null;
    this.stopPromise = new Promise(resolve => { this.resolveStop = resolve; });
  }

  log(message, fields = {}, level = 'INFO') {
    const details = Object.entries(fields)
      .map(([key, value]) => `${key}=${String(value).replace(/[\r\n]/g, ' ')}`).join(' ');
    console[level === 'ERROR' ? 'error' : 'log'](
      `${new Date().toISOString()} ${level} [${this.service}] ${message}${details ? ' | ' + details : ''}`
    );
  }

  reason(error) {
    return String(error?.message || error)
      .replace(/amqps?:\/\/[^\s]+/g, '[AMQP_URL]')
      .replace(/(password[=:]\s*)[^\s,}]+/gi, '$1[hidden]');
  }

  stop(reason) {
    if (this.stopping) return;
    this.stopping = true;
    this.log('Остановка запрошена', { reason });
    this.controller.abort();
    this.resolveStop();
    this.deadline = setTimeout(() => {
      this.log('Истёк тайм-аут остановки', { timeout_ms: 12000 }, 'ERROR');
      process.exit(1);
    }, 12000);
    this.deadline.unref();
  }

  fail(error, fields = {}) {
    if (!this.fault) {
      this.fault = error;
      this.log('Ошибка приложения', { ...fields, reason: this.reason(error) }, 'ERROR');
    }
    this.stop('error');
  }

  async pause(ms) {
    await delay(ms, undefined, { signal: this.controller.signal });
  }

  async connect(confirm = false) {
    const url = process.env.AMQP_URL || 'amqp://guest:guest@rabbitmq:5672?heartbeat=5';
    for (let attempt = 1; attempt <= 10; attempt++) {
      if (this.stopping) throw new Error('Подключение отменено');
      try {
        this.connection = await amqp.connect(url, { timeout: 3000 });
        break;
      } catch (error) {
        this.log('Брокер пока недоступен', {
          operation: 'connect', attempt, max_attempts: 10, reason: this.reason(error)
        }, 'WARN');
        if (attempt === 10) throw new Error('Исчерпаны 10 попыток подключения к брокеру');
        await this.pause(1000);
      }
    }
    this.watch(this.connection, 'connection');
    this.channel = confirm
      ? await this.connection.createConfirmChannel()
      : await this.connection.createChannel();
    this.watch(this.channel, 'channel');
    this.confirm = confirm;
    if (confirm) {
      this.channel.on('return', message => {
        const id = message.properties.correlationId || message.properties.messageId;
        const error = new Error(`Сообщение возвращено: ${message.fields.replyText}`);
        if (this.publication?.id === id) this.publication.returned = error;
        this.fail(error, { operation: 'return', message_id: id });
      });
    }
    return this.channel;
  }

  watch(resource, name) {
    resource.on('error', error => this.fail(error, { operation: name }));
    resource.on('close', () => {
      if (resource === this.channel) this.channelClosed = true;
      if (resource === this.connection) this.connectionClosed = true;
      if (!this.closing) this.fail(new Error('Неожиданное закрытие'), { operation: name });
    });
  }

  async publish(exchange, key, body, properties) {
    if (this.stopping) throw new Error('Публикация отменена');
    const state = { id: properties.messageId, returned: null };
    this.publication = state;
    const ch = this.channel;
    await new Promise((resolve, reject) => {
      let confirmed = false;
      let drained = false;
      const timer = setTimeout(() => finish(new Error('Тайм-аут publisher confirm/drain')), 10000);
      const onClose = () => finish(new Error('Канал закрыт до завершения публикации'));
      const onError = error => finish(error);
      const onDrain = () => { drained = true; check(); };
      const cleanup = () => {
        clearTimeout(timer);
        ch.off('close', onClose);
        ch.off('error', onError);
        ch.off('drain', onDrain);
      };
      let settled = false;
      const finish = error => {
        if (settled) return;
        settled = true;
        cleanup();
        if (error) reject(error); else resolve();
      };
      const check = () => {
        if (confirmed && drained) finish(state.returned);
      };
      ch.once('close', onClose);
      ch.once('error', onError);
      ch.once('drain', onDrain);
      try {
        const writable = ch.publish(exchange, key, Buffer.from(JSON.stringify(body)), {
          persistent: true, mandatory: true, contentType: 'application/json', ...properties
        }, error => {
          if (error) return finish(new Error(`Публикация отклонена: ${this.reason(error)}`));
          confirmed = true;
          check();
        });
        drained = writable;
        if (!writable) this.log('Ожидание освобождения буфера', { message_id: state.id });
        check();
      } catch (error) {
        finish(error);
      }
    });
    this.publication = null;
  }

  async consume(queue, handler) {
    const { consumerTag } = await this.channel.consume(queue, message => {
      if (!message) {
        if (!this.stopping) this.fail(new Error('Брокер отменил подписку'), { queue });
        return;
      }
      const job = Promise.resolve().then(() => handler(message)).catch(error => {
        this.fail(error, {
          operation: 'consume', queue,
          message_id: message.properties.messageId || 'unknown'
        });
      }).finally(() => this.jobs.delete(job));
      this.jobs.add(job);
    }, { noAck: false });
    this.tags.push(consumerTag);
    await writeFile(READY, 'ready\n');
  }

  async shutdown() {
    await rm(READY, { force: true });
    if (this.channel && !this.channelClosed) {
      for (const tag of this.tags) await this.channel.cancel(tag);
    }
    await Promise.all([...this.jobs]);
    if (this.confirm && this.channel && !this.channelClosed) {
      await this.channel.waitForConfirms();
    }
    this.closing = true;
    if (this.channel && !this.channelClosed) await this.channel.close();
    if (this.connection && !this.connectionClosed) await this.connection.close();
    this.log('Канал и соединение закрыты', { exit_code: this.fault ? 1 : 0 });
  }
}

export async function run(application) {
  const rt = new Runtime();
  const onTerm = () => rt.stop('SIGTERM');
  const onInt = () => rt.stop('SIGINT');
  process.on('SIGTERM', onTerm);
  process.on('SIGINT', onInt);
  try {
    await application(rt);
  } catch (error) {
    if (!(rt.stopping && !rt.fault && (error.name === 'AbortError' ||
      error.message === 'Подключение отменено' || error.message === 'Публикация отменена'))) {
      rt.fail(error, { operation: 'main' });
    }
  } finally {
    if (!rt.stopping) rt.stop('completed');
    try {
      await rt.shutdown();
    } catch (error) {
      rt.fail(error, { operation: 'shutdown' });
      rt.closing = true;
      if (rt.connection && !rt.connectionClosed) {
        try { await rt.connection.close(); } catch {}
      }
    }
    clearTimeout(rt.deadline);
    process.off('SIGTERM', onTerm);
    process.off('SIGINT', onInt);
    process.exitCode = rt.fault ? 1 : 0;
  }
}

