// CivicPlus feed poller.
// Checks Park County's emergency management RSS feed. On first run ever,
// it just records the current newest item as the baseline (sends nothing).
// On later runs, anything newer than the stored baseline triggers a real
// alert email via submit-alert, then updates the baseline.

import { createClient } from "@supabase/supabase-js";
import ws from "ws";
import { XMLParser } from "fast-xml-parser";

const FEED_URL =
  "https://www.parkcountyco.gov/RSSFeed.aspx?ModID=1&CID=Emergency-Management-10";

const SUPABASE_URL = process.env.SUPABASE_URL;
const SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const SUBMIT_API_KEY = process.env.SUBMIT_API_KEY;

const supabase = createClient(SUPABASE_URL, SERVICE_ROLE_KEY, {
  realtime: { transport: ws },
});

function stripTags(html) {
  return (html || "").replace(/<[^>]*>/g, "").trim();
}

async function submitAlert(item) {
  const res = await fetch(`${SUPABASE_URL}/functions/v1/submit-alert`, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${SUBMIT_API_KEY}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      subject: item.title,
      body_html: item.description || `<p>${item.title}</p>`,
      body_text: stripTags(item.description) || item.title,
      segment: "alerts",
      source_url: item.link,
    }),
  });

  if (!res.ok) {
    const text = await res.text();
    throw new Error(`submit-alert failed (${res.status}): ${text}`);
  }
  return res.json();
}

async function run() {
  const res = await fetch(FEED_URL);
  if (!res.ok) {
    throw new Error(`Failed to fetch feed: ${res.status}`);
  }
  const xml = await res.text();

  const parser = new XMLParser();
  const parsed = parser.parse(xml);
  const rawItems = parsed?.rss?.channel?.item;
  const items = Array.isArray(rawItems) ? rawItems : rawItems ? [rawItems] : [];

  console.log(`Raw item count: ${items.length}`);
  if (items.length > 0) {
    console.log("First raw item (debug):", JSON.stringify(items[0], null, 2));
  }

  const normalized = items
    .map((item) => ({
      title: item.title ?? "(untitled)",
      link: item.link ?? "",
      description: item.description ?? "",
      guid: (item.guid && (item.guid["#text"] ?? item.guid)) || item.link || "",
      pubDate: item.pubDate ? new Date(item.pubDate) : null,
    }))
    .filter((item) => item.pubDate !== null)
    .sort((a, b) => a.pubDate - b.pubDate);

  if (normalized.length === 0) {
    console.log("Feed returned no items with a valid date — nothing to do.");
    return;
  }

  const { data: state, error: stateError } = await supabase
    .from("feed_state")
    .select("last_item_guid, last_item_date")
    .eq("id", "civicplus")
    .maybeSingle();

  if (stateError) throw stateError;

  const newest = normalized[normalized.length - 1];

  if (!state) {
    console.log(
      "No baseline yet — seeding with the current newest item, sending nothing this run."
    );
    const { error: insertError } = await supabase.from("feed_state").insert({
      id: "civicplus",
      last_item_guid: newest.guid,
      last_item_date: newest.pubDate.toISOString(),
    });
    if (insertError) throw insertError;
    console.log(`Baseline set to: "${newest.title}" (${newest.pubDate.toISOString()})`);
    return;
  }

  const lastDate = state.last_item_date ? new Date(state.last_item_date) : null;
  const toSend = normalized.filter(
    (item) => !lastDate || item.pubDate > lastDate
  );

  if (toSend.length === 0) {
    console.log("No new items since last check.");
    return;
  }

  console.log(`${toSend.length} new item(s) found — sending in order.`);

  for (const item of toSend) {
    try {
      const result = await submitAlert(item);
      console.log(`Sent: "${item.title}" -> alert_id ${result.alert_id}`);

      const { error: updateError } = await supabase
        .from("feed_state")
        .update({
          last_item_guid: item.guid,
          last_item_date: item.pubDate.toISOString(),
          updated_at: new Date().toISOString(),
        })
        .eq("id", "civicplus");
      if (updateError) throw updateError;
    } catch (err) {
      console.error(`Failed to send "${item.title}":`, err.message || err);
      console.error("Stopping here — remaining items will be retried next run.");
      break;
    }
  }
}

run().catch((err) => {
  console.error("CivicPlus poller failed:", err);
  process.exit(1);
});
