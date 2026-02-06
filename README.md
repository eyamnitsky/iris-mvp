Iris — AI-Powered Email Scheduling Assistant

Overview

Iris is an AI-powered, transactional email scheduling assistant that coordinates meetings directly from email threads.

Iris is designed to behave like a human executive assistant:
	•	She participates only when explicitly included on an email thread
	•	She coordinates one-to-one and multi-participant meetings
	•	She sends transactional replies and calendar invitations only in response to inbound emails

This repository represents an advanced proof-of-concept focused on correctness, safety, and realistic email behavior rather than end-user polish.

⸻

Core Capabilities

Inbound Email Processing
	•	Receives emails via Amazon SES (Inbound)
	•	Stores raw RFC-822 messages in Amazon S3
	•	Extracts sender, recipients, subject, body, and threading headers

Thread-Aware Conversation Handling
	•	Canonical thread resolution across all participants
	•	Robust handling of:
	•	Message-Id
	•	In-Reply-To
	•	References
	•	SES-generated message IDs
	•	Replies from different participants are correctly resolved into the same logical thread

Single-Participant Scheduling
	•	Handles direct scheduling requests (e.g. “Schedule something tomorrow”)
	•	Uses AI to interpret natural language
	•	Defaults to 30-minute meetings unless otherwise specified

Multi-Participant Coordination

When Iris is included on an email with multiple participants, she:
	1.	Detects a coordination request
	2.	Identifies all required participants from the original message
	3.	Requests availability from each participant (with suggested formats)
	4.	Waits for all participants to respond
	5.	Reconciles availability
	6.	Schedules the meeting and sends calendar invitations

Key rules:
	•	Availability ranges (e.g. “2–4pm”) are not meeting duration
	•	Meeting duration defaults to 30 minutes unless explicitly specified
	•	Iris never schedules until all participants have responded

⸻

AI-Driven Understanding

Iris uses a large language model (LLM) to interpret natural language such as:
	•	“Any afternoon Mon–Tue next week”
	•	“After 3, but not during pickup”
	•	“Friday works, Saturday maybe”

The LLM is responsible for interpretation and normalization, not execution.

Deterministic code remains responsible for:
	•	Conflict detection
	•	Availability intersection
	•	Final scheduling decisions

⸻

AI Reasoning Mode (Optional)

An optional AI Reasoning Mode allows the LLM to:
	•	Consider all participant responses holistically
	•	Propose a concrete meeting slot (or alternatives)
	•	Identify missing or ambiguous responses

Even in this mode:
	•	Iris validates all proposals deterministically
	•	Iris never schedules invalid or unsafe times
	•	Deterministic fallback logic remains in place

Enable via environment variable:

AI_REASONING_MODE=true


⸻

Architecture

High-Level System Flow

flowchart LR
    A[Inbound Email]
    A --> B[Amazon SES (Inbound)]
    B --> C[Amazon S3\nRaw Email Storage]
    C --> D[AWS Lambda\nIris Handler]

    D --> E[LLM\nParsing & Reasoning]
    D --> F[DynamoDB\nThreads & State]
    F --> D

    D --> G[Amazon SES (Outbound)]
    G --> H[Recipients / Participants]

Key Components

Amazon SES (Inbound)
	•	Receives transactional emails
	•	Enforces strict receipt rules

Amazon S3
	•	Stores raw inbound messages

AWS Lambda (Python)
	•	Entry point for all processing
	•	Thread resolution
	•	Coordination logic
	•	AI integration

DynamoDB
	•	Thread state storage
	•	Participant tracking
	•	Coordination lifecycle

LLM Integration
	•	Natural language understanding
	•	Availability parsing
	•	Optional reconciliation proposals

Amazon SES (Outbound)
	•	Sends replies and calendar invites using SendRawEmail

⸻

Email Compliance & Safety

This service is strictly transactional:
	•	Emails are sent only in response to inbound messages
	•	Recipients are limited to:
	•	The original sender
	•	Participants already present on the thread
	•	No marketing, bulk, or cold outreach
	•	No tracking pixels or analytics
	•	No reuse of recipient lists

Designed to comply with:
	•	AWS SES Transactional Email Policy
	•	AWS Acceptable Use Policy

⸻

Current Status
	•	✅ Domain verified in SES
	•	✅ Inbound and outbound email operational
	•	✅ Calendar invitations render correctly (Gmail, Apple Mail, Outlook)
	•	✅ Multi-participant coordination functional
	•	⚠️ Ongoing iteration on AI reconciliation quality

This is not yet a production SaaS.

⸻

Non-Goals (For Now)
	•	No calendar API integrations (Google / Microsoft)
	•	No user-facing UI
	•	No background reminders or nudges
	•	No scheduling without explicit participant replies

⸻

License

This project is licensed under the Polyform Noncommercial License.

You are free to explore, modify, and use the code for non-commercial purposes.
Commercial use requires explicit permission from the author.

⸻

Contact

Repository Owner: Eugene Yamnitsky
Email: eugene.yamnitsky@gmail.com