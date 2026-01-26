db.transactions.aggregate([
  { $match: { status: "completed" } },
  { $unwind: "$items" },
  {
    $group: {
      _id: "$items.product_id",
      revenue: { $sum: "$items.subtotal" },
      units: { $sum: "$items.quantity" }
    }
  },
  {
    $lookup: {
      from: "products",
      localField: "_id",
      foreignField: "product_id",
      as: "p"
    }
  },
  { $unwind: "$p" },
  {
    $group: {
      _id: "$p.category_id",
      revenue: { $sum: "$revenue" },
      units: { $sum: "$units" }
    }
  },
  { $sort: { revenue: -1 } },
  { $limit: 10 }
])
