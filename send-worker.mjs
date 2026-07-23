// Fire Alert send worker.
// Runs on a schedule via GitHub Actions. Sends as many queued emails
// as the configured rate limits allow, then stops. Does nothing at
// all unless SENDING_ENABLED is exactly "true".

import { createClient } from "@supabase/supabase-js";
import nodemailer from "nodemailer";

const SENDING_ENABLED = process.env.SENDING_ENABLED === "true";
const RATE_LIMIT_PER_HOUR = parseInt(process.env.RATE_LIMIT_PER_HOUR || "0", 10);
const RATE_LIMIT_PER_DAY = parseInt(process.env.RATE_LIMIT_PER_DAY || "0", 10);

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY
);

const transporter = nodemailer.createTransport({
  host: process.env.SMTP_HOST,
  port: parseInt(process.env.SMTP_PORT || "587", 10),
  secure: process.env.SMTP_PORT === "465",
  auth: {
    user: process.env.SMTP_USER,
    pass: process.env.SMTP_PASS,
  },
});

async function countSentSince(hoursAgo) {
  const since = new Date(Date.now() - hoursAgo * 60 * 60 * 1000).toISOString();
  const { count, error } = await supabase
    .from("send_queue")
    .select("id", { count: "exact", head: true })
    .eq("status", "sent")
    .gte("sent_at", since);
  if (error) throw error;
  return count ?? 0;
}

async function run() {
  if (!SENDING_ENABLED) {
    console.log("SENDING_ENABLED is not \"true\" — exiting without sending anything.");
    return;
  }

  const sentLastHour = await countSentSince(1);
  const sentLastDay = await countSentSince(24);

  const roomHour = RATE_LIMIT_PER_HOUR - sentLastHour;
  const roomDay = RATE_LIMIT_PER_DAY - sentLastDay;
  const room = Math.max(0, Math.min(roomHour, roomDay));

  console.log(
    `Sent in last hour: ${sentLastHour}/${RATE_LIMIT_PER_HOUR}, last 24h: ${sentLastDay}/${RATE_LIMIT_PER_DAY}. Room this run: ${room}`
  );

  if (room <= 0) {
    console.log("No room left under current rate limits — exiting.");
    return;
  }

  const { data: queueRows, error: queueError } = await supabase
    .from("send_queue")
    .select("id, alert_id, subscriber_id, created_at")
    .eq("status", "pending")
    .order("created_at", { ascending: true })
    .limit(room);

  if (queueError) throw queueError;

  if (!queueRows || queueRows.length === 0) {
    console.log("Queue is empty — nothing to send.");
    return;
  }

  console.log(`Sending ${queueRows.length} email(s)...`);

  for (const row of queueRows) {
    try {
      const { data: alert, error: alertErr } = await supabase
        .from("alerts")
        .select("subject, body_html, body_text")
        .eq("id", row.alert_id)
        .single();
      if (alertErr) throw alertErr;

      const { data: subscriber, error: subErr } = await supabase
        .from("subscribers")
        .select("email, unsubscribe_token")
        .eq("id", row.subscriber_id)
        .single();
      if (subErr) throw subErr;

      const unsubscribeUrl = `${process.env.SUPABASE_URL}/functions/v1/unsubscribe?token=${subscriber.unsubscribe_token}`;

      const htmlWithFooter = `${alert.body_html}<hr><p style="font-size:12px;color:#888">You're receiving this because you signed up for Fire Adapted Park County alerts. <a href="${unsubscribeUrl}">Unsubscribe</a></p>`;
      const textWithFooter = `${alert.body_text || ""}\n\n---\nUnsubscribe: ${unsubscribeUrl}`;

      await transporter.sendMail({
        from: process.env.SMTP_FROM,
        to: subscriber.email,
        subject: alert.subject,
        html: htmlWithFooter,
        text: textWithFooter,
      });

      await supabase
        .from("send_queue")
        .update({ status: "sent", sent_at: new Date().toISOString() })
        .eq("id", row.id);

      console.log(`Sent to ${subscriber.email}`);
    } catch (err) {
      console.error(`Failed to send send_queue row ${row.id}:`, err.message || err);
      // Left as "pending" on failure, so it's retried on the next run.
    }
  }

  // Mark any alerts that are now fully drained as sent.
  const alertIds = [...new Set(queueRows.map((r) => r.alert_id))];
  for (const alertId of alertIds) {
    const { count } = await supabase
      .from("send_queue")
      .select("id", { count: "exact", head: true })
      .eq("alert_id", alertId)
      .eq("status", "pending");
    if (count === 0) {
      await supabase.from("alerts").update({ status: "sent" }).eq("id", alertId);
    }
  }
}

run().catch((err) => {
  console.error("Send worker failed:", err);
  process.exit(1);
});
