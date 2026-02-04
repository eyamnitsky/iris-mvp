# Iris – Transactional Email Scheduling Assistant (POC)

## Overview

This repository contains a **proof-of-concept transactional email service** named **Iris**.  
Iris acts as an automated scheduling assistant that processes inbound emails and sends
calendar invitations and replies on behalf of a user.

The system is designed to support **one-to-one, user-initiated scheduling workflows**
(e.g. meeting coordination, replies, and rescheduling), **not marketing or bulk email**.

This repository is currently used for internal testing and validation only.

---

## What This Service Does

1. **Receives inbound emails** sent to a managed domain (e.g. `iris@<domain>`)
2. **Parses the message content** (sender, recipients, subject, body)
3. **Applies scheduling logic** (e.g. generate a calendar invite or reply)
4. **Sends transactional responses**, including:
   - Calendar invitations (`.ics`)
   - Direct replies to the original participants

All outbound emails are generated **only in response to an inbound email** and only to
addresses explicitly included in that conversation.

---

## What This Service Does *Not* Do

- ❌ No marketing emails  
- ❌ No bulk messaging  
- ❌ No cold outreach  
- ❌ No mailing lists  
- ❌ No third-party address acquisition  

Every outbound email is **transactional, contextual, and user-initiated**.

---

## Email Compliance & Consent Model

- Emails are sent **only after a user initiates contact**
- Recipients are limited to:
  - The original sender
  - Participants already included on the email thread
- No persistence of recipient lists for reuse or campaigns
- No tracking pixels or marketing analytics

This aligns with AWS SES **Transactional Email** usage guidelines.

---

## Architecture (High Level)

- **Amazon SES (Inbound)**  
  Receives incoming emails via receipt rules

- **Amazon S3**  
  Stores raw inbound email messages for processing

- **AWS Lambda (Python)**  
  Parses inbound messages, applies logic, and generates responses

- **Amazon SES (Outbound)**  
  Sends transactional replies and calendar invites (`SendRawEmail`)

---

## Current Status

- Domain verified in SES
- Inbound email processing operational
- Outbound transactional emails operational
- Calendar invitations successfully delivered and rendered by major providers (e.g. Gmail)

This repository represents an **early-stage prototype**, not a public-facing production service.

---

## Intended Use

The long-term goal is to support individuals and small teams who explicitly choose to use
Iris as a scheduling assistant by routing emails through their own domain.

Future versions may support:
- User-owned sending domains
- Explicit user onboarding
- Per-user configuration and controls

---

## Contact

For questions related to this proof-of-concept or SES usage review, please contact:

**Repository Owner:**  
Eugene Yamnitsky  
Email: eugene.yamnitsky@gmail.com

---

## Acknowledgment

This service complies with:
- AWS Service Terms
- AWS Acceptable Use Policy (AUP)
- AWS SES Transactional Email guidelines

