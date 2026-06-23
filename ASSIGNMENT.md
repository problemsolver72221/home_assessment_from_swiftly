# Device Control Pipeline Simulator

## Background

This exercise is inspired by the kind of work our team does: building software that runs on hardware onboard transit vehicles and communicates with cloud systems. The pipeline you'll build here is a stripped-down model of a real problem we work on every day. The shape is simple: control messages originating from a cloud service, a processing layer, and a sidecar service that does something useful with the results. The constraints behind it are not simple, however, and we want you to feel some of those constraints when you read the next section.

## The Real-World Context

The production version of this pipeline runs both in our cloud and on hardware physically bolted onboard transit vehicles. That changes almost everything about what it means for the software to be "working."

Vehicles move. Cellular connectivity drops out in tunnels, parking structures, depots, and dead zones along a route. Outages can last seconds or hours. Vehicles also power-cycle on a schedule that has nothing to do with our software and our processes need to come back up cleanly without losing whatever they were in the middle of.

A fleet can be up to hundreds of vehicles, each running its own install of software represented here by the sidecar and consumer. Messages are vehicle-scoped: a control message intended for one bus is meaningless to another. Operations staff need to be able to look at the fleet and quickly tell which vehicles are healthy and which are silently failing.

The sidecar, in the real version, often bridges to onboard hardware that speaks something other than TCP: RS485, J1708, CAN bus, or other vendor-specific serial protocols. Deploys happen while vehicles are in revenue service. Security matters because the network is not managed by us and the hardware is potentially physically accessible to non-agency actors.

We *don't* expect a take-home to address all of this. We *do* expect you to read this section, recognize that the floor we've specified is just the floor, and pick a direction from this reality to push on.

## Your Task

You are given a `docker-compose.yml` (included in this packet) that wires up all the required components. Your job is to implement the three Python scripts it references. You may restructure or extend the compose file if you like, but the system must still start with `docker compose up`.

You may use any Python libraries you like. You may also swap Python for another language; just update the `docker-compose.yml` accordingly.

---

## Components

### 1. Producer

Runs on a 1-second cadence. Each tick, it publishes a message to the MQTT broker on topic `control/raw`. Each message must include a unique ID, a random string of 3–5 uppercase letters, a random integer between 1 and 200, and a timestamp.

It also subscribes to `control/error` and logs any error messages from the pipeline.

### 2. Consumer

Subscribes to `control/raw`. For each message, it should forward it to the Sidecar over TCP on port 9000 for validation. The wire protocol is up to you; document your design decisions in the README. Note that the sidecar needs to be able to restart independently of the consumer. Take care that messages are not silently lost when this happens.

If the Sidecar determines a message has failed validation, the Consumer is responsible for publishing an error to `control/error`. The error message must convey the original message ID, the letters, the computed sum as returned in the Sidecar's response, the original number, and both the original timestamp and the timestamp when the error was generated.

### 3. Sidecar

Listens on TCP port 9000 for incoming messages from the Consumer. The Sidecar is the authoritative validator: it computes the **letter sum** (A=1, B=2, … Z=26) and determines whether the sum is greater than the number. It must communicate the result of this check back to the Consumer. For each message received, it should:

- Decode the message. Log a warning and discard if malformed
- Track and log metrics that would help you diagnose issues with this pipeline in production. Document in your README what you chose to track and why.

### 4. MQTT Broker

Provided for you via Mosquitto in the compose file. No implementation needed.

---

## Starter Files

The following starter files are included alongside this document. Implement the three Python scripts; the compose file and broker config are provided as-is.

- `docker-compose.yml`: wires up all four components (provided, no changes required to run)
- `producer.py`: stub to implement
- `consumer.py`: stub to implement
- `sidecar.py`: stub to implement

---

## Requirements

- We expect this to take roughly 3–4 hours of focused work. If you're running low on time add comments to the code and/or notes in the Readme indicating what scope was cut and what work would theoretically come next.
- The entire system must start with `docker compose up`
- Include a README that documents not only setup and usage instructions, but also your design decisions and any other relevant information. The spec intentionally leaves many details unspecified, and we'd like to understand your approach, along with any significant choices and tradeoffs you made.
- **Pick a direction and extend.** The components above are the floor. Pick something from the real world context that interests you or propose your own direction (with reasoning for why it's worth pushing on) and push on it. We'd rather see one extension explored thoughtfully than the floor done perfectly. In your README, tell us what you extended toward, why that direction, what trade-offs you weighed, and what you'd do next if you had more time. The size of the extension matters less than the quality of the thinking behind it.
- AI tools are allowed and encouraged. If you use one or more, include in your README which tools were used and how they were used (testing, design, implementation, documentation, planning, etc).
