const express = require("express");
const axios = require("axios");
const cors = require("cors");

const app = express();
app.use(express.json());
app.use(cors());

// TEST BUYERS
let buyers = [
  {
    id: 1,
    name: "Test Buyer A",
    api_url: "https://webhook.site/7f50c212-a9b0-4ec4-94a3-62d1b871d051",
    is_active: true,
    priority: 1,
    timeout_ms: 800,
    daily_cap: 100,
    current_count: 0,
    payout: 25
  },
  {
    id: 2,
    name: "Test Buyer B",
    api_url: "https://webhook.site/cf8dff0d-7f69-43f9-bc3d-c794f355f40b",
    is_active: true,
    priority: 2,
    timeout_ms: 800,
    daily_cap: 100,
    current_count: 0,
    payout: 20
  }
];

function getActiveBuyers() {
  return buyers
    .filter(b => b.is_active && b.current_count < b.daily_cap)
    .sort((a, b) => a.priority - b.priority);
}

async function sendPing(buyer, data) {
  try {
    await axios.post(buyer.api_url, data, {
      timeout: buyer.timeout_ms
    });

    return {
      accepted: true,
      payout: buyer.payout
    };
  } catch (err) {
    console.log(`Ping failed for ${buyer.name}:`, err.message);
    return { accepted: false, payout: 0 };
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

app.get("/", (req, res) => {
  res.json({
    status: "Ping tree server is running",
    buyers
  });
});

app.post("/api/lead", async (req, res) => {
  const { affiliate_id, data } = req.body;

  if (!affiliate_id || !data) {
    return res.status(400).json({
      status: "error",
      message: "affiliate_id and data are required"
    });
  }

  const activeBuyers = getActiveBuyers();
  let winner = null;
  let winningPing = null;
  let pingLog = [];

  for (const buyer of activeBuyers) {
    const pingData = {
      postcode: data.postcode,
      loan_amount: data.loan_amount
    };

    const pingResponse = await sendPing(buyer, pingData);

    pingLog.push({
      buyer: buyer.name,
      accepted: pingResponse.accepted,
      payout: pingResponse.payout
    });

    if (pingResponse.accepted) {
      winner = buyer;
      winningPing = pingResponse;
      buyer.current_count += 1;
      break;
    }
  }

  if (winner) {
    const posted = await sendPost(winner, data);

    return res.json({
      status: "accepted",
      buyer: winner.name,
      payout: winningPing.payout,
      posted,
      ping_log: pingLog
    });
  }

  return res.json({
    status: "rejected",
    ping_log: pingLog
  });
});

const PORT = process.env.PORT || 3000;

app.listen(PORT, () => {
  console.log(`Ping tree server running on port ${PORT}`);
});
