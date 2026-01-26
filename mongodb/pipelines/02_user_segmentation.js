db.transactions.aggregate([
  { $match: { status: "completed" } },
  {
    $group: {
      _id: "$user_id",
      orders: { $sum: 1 },
      total_spent: { $sum: "$total" },
      last_purchase: { $max: "$timestamp" }
    }
  },
  {
    $addFields: {
      segment: {
        $switch: {
          branches: [
            { case: { $gte: ["$orders", 10] }, then: "High Frequency" },
            { case: { $gte: ["$orders", 4] }, then: "Medium Frequency" }
          ],
          default: "Low Frequency"
        }
      }
    }
  },
  {
    $group: {
      _id: "$segment",
      users: { $sum: 1 },
      avg_orders: { $avg: "$orders" },
      avg_spent: { $avg: "$total_spent" }
    }
  },
  { $sort: { users: -1 } }
])
