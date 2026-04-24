const express = require("express");
const axios = require("axios");
const cors = require("cors");

const app = express();
app.use(express.json());
app.use(cors());

let buyers = [
  {
    id: 1,
    name: "Test Buyer A",
    api_url: "https://webhook.site/7f50c212-a9b0-4ec4-94a3-62d1b871d051",
    is_active: true,
    priority: 1,
    timeout_ms: 800,
    daily_cap: 100,
    current_count: 0
  }
];

function getActiveBuyers() {
  return buyers
    .filter(b => b.is_active && b.current_count < b.daily_cap)
    .sort((a, b) => a.priority - b.priority);
}

async function sendPing(buyer, data) {
  try {
    const res = await axios.post(buyer.api_url, data, {
      timeout: buyer.timeout_ms
    });

 return {
  accepted: true,
  payout: 25
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
  res.json({ status: "Ping tree server is running" });
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

  for (const buyer of activeBuyers) {
    const pingResponse = await sendPing(buyer, {
      postcode: data.postcode,
      loan_amount: data.loan_amount
    });

    if (pingResponse.accepted) {
      winner = buyer;
      buyer.current_count += 1;
      break;
    }
  }

  if (winner) {
    await sendPost(winner, data);

    return res.json({
      status: "accepted",
      buyer: winner.name
    });
  }

  return res.json({
    status: "rejected"
  });
});

const PORT = process.env.PORT || 3000;

app.listen(PORT, () => {
  console.log(`Ping tree server running on port ${PORT}`);
});
