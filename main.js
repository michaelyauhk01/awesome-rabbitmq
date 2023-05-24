import express from "express";
import bodyParser from "body-parser";
import amqplib from "amqplib";
import path from "path";

const main = async () => {
  const EXAMPLE_QUEUE = "example_queue";
  const EXAMPLE_WORK_QUEUE = "example_work_queue";
  const FANOUT_EXCHANGE = "fanout_exchange";
  const DIRECT_EXCHANGE = "direct_exchange";

  const client = await amqplib.connect("amqp://127.0.0.1:5672");
  const channel = await client.createChannel();
  await channel.assertExchange(FANOUT_EXCHANGE, "fanout");
  await channel.assertExchange(DIRECT_EXCHANGE, "direct");

  await channel.assertQueue("log_error");
  await channel.assertQueue("log_info");

  await channel.bindQueue("log_error", DIRECT_EXCHANGE, "error");
  await channel.bindQueue("log_info", DIRECT_EXCHANGE, "info");

  await channel.assertQueue(EXAMPLE_QUEUE, { durable: true });
  await channel.assertQueue(EXAMPLE_WORK_QUEUE, { durable: true });
  const { queue: exchangeQ1 } = await channel.assertQueue("exchange_q_1", {
    exclusive: true,
  });
  const { queue: exchangeQ2 } = await channel.assertQueue("exchange_q_2", {
    exclusive: true,
  });
  await channel.bindQueue(exchangeQ1, FANOUT_EXCHANGE, "");
  await channel.bindQueue(exchangeQ2, FANOUT_EXCHANGE, "");

  const startProducerService = () => {
    const app = express();
    const port = 3000;

    app.use(bodyParser.json());

    app.get("/", (_, res) => {
      res.sendFile(path.join(__dirname, "public", "index.html"));
    });

    app.post("/event/send", async (req, res) => {
      try {
        const { message } = req.body;

        channel.sendToQueue(EXAMPLE_QUEUE, Buffer.from(message), {
          persistent: true,
        });
        res.status(201).end();
      } catch (e) {
        res.status(500).json({ stack: e });
      }
    });

    app.post("/event/send_work", async (req, res) => {
      try {
        const { message } = req.body;

        channel.sendToQueue(EXAMPLE_WORK_QUEUE, Buffer.from(message));

        res.status(201).end();
      } catch (e) {
        res.status(500).json({ stack: e });
      }
    });

    app.post("/event/publish/fanout", async (req, res) => {
      try {
        const { message } = req.body;

        channel.publish(FANOUT_EXCHANGE, "", Buffer.from(message));

        res.status(201).end();
      } catch (e) {
        console.log(e);
        res.status(500).json({ stack: e });
      }
    });

    app.post("/event/publish/direct", async (req, res) => {
      try {
        const { message, level } = req.body;

        channel.publish(DIRECT_EXCHANGE, level, Buffer.from(message));

        res.status(201).end();
      } catch (e) {
        res.status(500).json({ stack: e });
      }
    });

    app.listen(port, () => {
      console.log(`http://localhost:${port}`);
    });
  };

  const startConsumerService = () => {
    const app = express();
    const port = 4000;

    channel.consume(
      EXAMPLE_QUEUE,
      (msg) => {
        console.log(msg.content.toString());
        channel.ack(msg);
      },
      { persistent: true }
    );

    channel.consume(
      EXAMPLE_WORK_QUEUE,
      (msg) => {
        setTimeout(() => {
          console.log("expensive task done by consumer service");
          channel.ack(msg);
        }, 5 * 1000);
      },
      { persistent: true }
    );

    channel.consume(exchangeQ2, (msg) => {
      console.log(`Consumer received: `, msg.content.toString());
      channel.ack(msg);
    });

    app.get("/", (_, res) => {
      res.send("sub service");
    });

    app.listen(port, () => {
      console.log(`http://localhost:${port}`);
    });
  };

  const startWorker = async (port, number) => {
    const app = express();

    channel.consume(
      EXAMPLE_WORK_QUEUE,
      (msg) => {
        setTimeout(() => {
          console.log(`expensive task done by worker ${number}`);
          channel.ack(msg);
        }, 5 * 1000);
      },
      { persistent: true }
    );

    channel.consume(exchangeQ1, (msg) => {
      console.log(
        `Worker ${number} consumed message: `,
        msg.content.toString()
      );
      channel.ack(msg);
    });

    channel.consume("log_info", (msg) => {
      console.log(msg.content.toString());
      channel.ack(msg);
    });

    app.listen(port, () => {
      console.log(`http://localhost:${port}`);
    });
  };

  startWorker(4001, 0);
  startWorker(4002, 1);
  startWorker(4003, 2);
  startProducerService();
  startConsumerService();
};

main();
