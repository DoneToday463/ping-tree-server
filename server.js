const express = require("express");
const axios = require("axios");
const cors = require("cors");
const { Pool } = require("pg");

const app = express();
app.use(express.json());
app.use(cors());

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.DATABASE_URL ? { rejectUnauthorized: false } : false
});

async function initDb() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS buyers (
      id SERIAL PRIMARY KEY,
      name TEXT NOT NULL,
      api_url TEXT NOT NULL,
      is_active BOOLEAN DEFAULT true,
      priority INTEGER DEFAULT 1,
      timeout_ms INTEGER DEFAULT 800,
      daily_cap INTEGER DEFAULT 100,
      current_count INTEGER DEFAULT 0,
      payout NUMERIC DEFAULT 0,
      min_loan_amount NUMERIC DEFAULT 0,
      created_at TIMESTAMP DEFAULT NOW()
    );
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS leads (
      id SERIAL PRIMARY KEY,
      affiliate_id TEXT,
      buyer_id INTEGER,
      status TEXT,
      payout NUMERIC DEFAULT 0,
      posted BOOLEAN DEFAULT false,
      payload JSONB,
      created_at TIMESTAMP DEFAULT NOW()
    );
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS ping_logs (
      id SERIAL PRIMARY KEY,
      lead_id INTEGER,
      buyer_id INTEGER,
      buyer_name TEXT,
      accepted BOOLEAN,
      payout NUMERIC DEFAULT 0,
      reason TEXT,
      created_at TIMESTAMP DEFAULT NOW()
    );
  `);
  await pool.query(`
    CREATE TABLE IF NOT EXISTS affiliates (
      id SERIAL PRIMARY KEY,
      name TEXT NOT NULL,
      email TEXT,
      api_key TEXT UNIQUE,
      is_active BOOLEAN DEFAULT true,
      created_at TIMESTAMP DEFAULT NOW()
    );
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS offers (
      id SERIAL PRIMARY KEY,
      name TEXT NOT NULL,
      vertical TEXT,
      slug TEXT UNIQUE,
      is_active BOOLEAN DEFAULT true,
      payout NUMERIC DEFAULT 0,
      landing_url TEXT,
      created_at TIMESTAMP DEFAULT NOW()
    );
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS affiliate_offers (
      id SERIAL PRIMARY KEY,
      affiliate_id INTEGER,
      offer_id INTEGER,
      is_active BOOLEAN DEFAULT true,
      payout NUMERIC DEFAULT 0,
      created_at TIMESTAMP DEFAULT NOW()
    );
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS clicks (
      id SERIAL PRIMARY KEY,
      affiliate_id TEXT,
      offer_id INTEGER,
      click_id TEXT,
      source TEXT,
      ip_address TEXT,
      user_agent TEXT,
      created_at TIMESTAMP DEFAULT NOW()
    );
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS redirects (
      id SERIAL PRIMARY KEY,
      affiliate_id TEXT,
      offer_id INTEGER,
      click_id TEXT,
      redirect_url TEXT,
      created_at TIMESTAMP DEFAULT NOW()
    );
  `);
  const existing = await pool.query(`SELECT COUNT(*) FROM buyers;`);

  if (Number(existing.rows[0].count) === 0) {
    await pool.query(`
      INSERT INTO buyers 
      (name, api_url, is_active, priority, timeout_ms, daily_cap, current_count, payout, min_loan_amount)
      VALUES
      ('Test Buyer A', 'https://webhook.site/6a9269d6-cf72-4714-afde-d62a9d586e04', true, 1, 800, 100, 0, 25, 6000),
      ('Test Buyer B', 'https://webhook.site/bffc1662-20ed-4afd-9eb0-3bd1e32a4577', true, 2, 800, 100, 0, 20, 4000),
      ('Test Buyer C', 'https://webhook.site/3d908d77-3117-4e95-95ee-b05461004f95', true, 3, 800, 100, 0, 30, 8000);
    `);
  }

  console.log("Database ready");
}

async function getActiveBuyers() {
  const result = await pool.query(`
    SELECT *
    FROM buyers
    WHERE is_active = true
    AND current_count < daily_cap
    ORDER BY priority ASC;
  `);

  return result.rows;
}

async function sendPing(buyer, data) {
  try {
    const response = await axios.post(buyer.api_url, data, {
      timeout: buyer.timeout_ms
    });

    if (response.data && typeof response.data.accepted === "boolean") {
      return {
        accepted: response.data.accepted,
        payout: response.data.accepted ? Number(response.data.payout || buyer.payout) : 0,
        reason: "Accepted/rejected by buyer API response"
      };
    }

    const accepted = Number(data.loan_amount) >= Number(buyer.min_loan_amount);

    return {
      accepted,
      payout: accepted ? Number(buyer.payout) : 0,
      reason: accepted
        ? "Accepted by fallback buyer rule"
        : `Rejected: loan amount below ${buyer.min_loan_amount}`
    };
  } catch (err) {
    return {
      accepted: false,
      payout: 0,
      reason: `Ping failed: ${err.message}`
    };
  }
}

async function sendPost(buyer, data) {
  try {
    await axios.post(buyer.api_url, data, {
      timeout: buyer.timeout_ms
    });

    return true;
  } catch (err) {
    console.log(`Post failed for ${buyer.name}:`, err.message);
    return false;
  }
}

app.get("/", async (req, res) => {
  const buyers = await pool.query(`SELECT * FROM buyers ORDER BY priority ASC;`);

  res.json({
    status: "Ping tree server is running",
    buyers: buyers.rows
  });
});

app.get("/admin/buyers", async (req, res) => {
  const result = await pool.query(`SELECT * FROM buyers ORDER BY priority ASC;`);
  res.json(result.rows);
});

app.post("/admin/buyers", async (req, res) => {
  const {
    name,
    api_url,
    is_active = true,
    priority = 1,
    timeout_ms = 800,
    daily_cap = 100,
    payout = 0,
    min_loan_amount = 0
  } = req.body;

  const result = await pool.query(
    `
    INSERT INTO buyers
    (name, api_url, is_active, priority, timeout_ms, daily_cap, payout, min_loan_amount)
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
    RETURNING *;
    `,
    [name, api_url, is_active, priority, timeout_ms, daily_cap, payout, min_loan_amount]
  );

  res.json(result.rows[0]);
});

app.patch("/admin/buyers/:id", async (req, res) => {
  const { id } = req.params;
  const fields = req.body;

  const allowed = [
    "name",
    "api_url",
    "is_active",
    "priority",
    "timeout_ms",
    "daily_cap",
    "current_count",
    "payout",
    "min_loan_amount"
  ];

  const updates = [];
  const values = [];

  Object.keys(fields).forEach((key) => {
    if (allowed.includes(key)) {
      values.push(fields[key]);
      updates.push(`${key} = $${values.length}`);
    }
  });

  if (updates.length === 0) {
    return res.status(400).json({ error: "No valid fields to update" });
  }

  values.push(id);

  const result = await pool.query(
    `
    UPDATE buyers
    SET ${updates.join(", ")}
    WHERE id = $${values.length}
    RETURNING *;
    `,
    values
  );

  res.json(result.rows[0]);
});
app.get("/admin/leads", async (req, res) => {
  const result = await pool.query(`
    SELECT
      leads.id,
      leads.affiliate_id,
      leads.status,
      leads.payout,
      leads.posted,
      leads.created_at,
      buyers.name AS buyer_name,
      leads.payload
    FROM leads
    LEFT JOIN buyers ON leads.buyer_id = buyers.id
    ORDER BY leads.created_at DESC
    LIMIT 100;
  `);

  res.json(result.rows);
});

app.get("/admin/leads/:id", async (req, res) => {
  const { id } = req.params;

  const leadResult = await pool.query(
    `
    SELECT
      leads.id,
      leads.affiliate_id,
      leads.status,
      leads.payout,
      leads.posted,
      leads.created_at,
      buyers.name AS buyer_name,
      leads.payload
    FROM leads
    LEFT JOIN buyers ON leads.buyer_id = buyers.id
    WHERE leads.id = $1;
    `,
    [id]
  );

  const logsResult = await pool.query(
    `
    SELECT *
    FROM ping_logs
    WHERE lead_id = $1
    ORDER BY created_at ASC;
    `,
    [id]
  );

  res.json({
    lead: leadResult.rows[0] || null,
    ping_logs: logsResult.rows
  });
});

app.get("/admin/ping-logs", async (req, res) => {
  const result = await pool.query(`
    SELECT *
    FROM ping_logs
    ORDER BY created_at DESC
    LIMIT 200;
  `);

  res.json(result.rows);
});
app.get("/admin/affiliates", async (req, res) => {
  const result = await pool.query(`SELECT * FROM affiliates ORDER BY id DESC;`);
  res.json(result.rows);
});

app.post("/admin/affiliates", async (req, res) => {
  const { name, email, api_key, is_active = true } = req.body;

  const result = await pool.query(
    `
    INSERT INTO affiliates (name, email, api_key, is_active)
    VALUES ($1, $2, $3, $4)
    RETURNING *;
    `,
    [name, email, api_key, is_active]
  );

  res.json(result.rows[0]);
});

app.get("/admin/offers", async (req, res) => {
  const result = await pool.query(`SELECT * FROM offers ORDER BY id DESC;`);
  res.json(result.rows);
});

app.post("/admin/offers", async (req, res) => {
  const {
    name,
    vertical,
    slug,
    is_active = true,
    payout = 0,
    landing_url
  } = req.body;

  const result = await pool.query(
    `
    INSERT INTO offers (name, vertical, slug, is_active, payout, landing_url)
    VALUES ($1, $2, $3, $4, $5, $6)
    RETURNING *;
    `,
    [name, vertical, slug, is_active, payout, landing_url]
  );

  res.json(result.rows[0]);
});
app.post("/api/lead", async (req, res) => {
  const { affiliate_id, data } = req.body;

  if (!affiliate_id || !data) {
    return res.status(400).json({
      status: "error",
      message: "affiliate_id and data are required"
    });
  }

  const leadResult = await pool.query(
    `
    INSERT INTO leads (affiliate_id, status, payload)
    VALUES ($1, $2, $3)
    RETURNING *;
    `,
    [affiliate_id, "received", data]
  );

  const lead = leadResult.rows[0];

  const activeBuyers = await getActiveBuyers();

  const pingData = {
    postcode: data.postcode,
    loan_amount: data.loan_amount
  };

  let pingLog = [];

  for (const buyer of activeBuyers) {
    const pingResponse = await sendPing(buyer, pingData);

    await pool.query(
      `
      INSERT INTO ping_logs
      (lead_id, buyer_id, buyer_name, accepted, payout, reason)
      VALUES ($1,$2,$3,$4,$5,$6);
      `,
      [
        lead.id,
        buyer.id,
        buyer.name,
        pingResponse.accepted,
        pingResponse.payout,
        pingResponse.reason
      ]
    );

    pingLog.push({
      buyer_id: buyer.id,
      buyer: buyer.name,
      accepted: pingResponse.accepted,
      payout: pingResponse.payout,
      reason: pingResponse.reason
    });
  }

  const acceptedBuyers = pingLog.filter(p => p.accepted);

  if (acceptedBuyers.length === 0) {
    await pool.query(
      `UPDATE leads SET status = $1 WHERE id = $2;`,
      ["rejected", lead.id]
    );

    return res.json({
      status: "rejected",
      lead_id: lead.id,
      ping_log: pingLog
    });
  }

  const winningPing = acceptedBuyers.sort((a, b) => b.payout - a.payout)[0];
  const winner = activeBuyers.find(b => b.id === winningPing.buyer_id);

  await pool.query(
    `UPDATE buyers SET current_count = current_count + 1 WHERE id = $1;`,
    [winner.id]
  );

  const posted = await sendPost(winner, data);

  await pool.query(
    `
    UPDATE leads
    SET status = $1, buyer_id = $2, payout = $3, posted = $4
    WHERE id = $5;
    `,
    ["accepted", winner.id, winningPing.payout, posted, lead.id]
  );

  return res.json({
    status: "accepted",
    lead_id: lead.id,
    buyer: winner.name,
    payout: winningPing.payout,
    posted,
    ping_log: pingLog
  });
});

const PORT = process.env.PORT || 3000;

initDb()
  .then(() => {
    app.listen(PORT, () => {
      console.log(`Ping tree server running on port ${PORT}`);
    });
  })
  .catch((err) => {
    console.error("Database init failed:", err);
    process.exit(1);
  });
