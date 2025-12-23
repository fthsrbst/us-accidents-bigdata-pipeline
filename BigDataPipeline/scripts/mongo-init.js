// MongoDB Initialization Script
// Creates database and user for the big data project

// Switch to bigdata_project database
db = db.getSiblingDB("bigdata_project");

// Create collections
db.createCollection("us_accidents_raw");
db.createCollection("us_accidents_cleaned");
db.createCollection("us_accidents_clustered");
db.createCollection("knn_results");
db.createCollection("kmeans_results");
db.createCollection("random_forest_results");
db.createCollection("pipeline_logs");

// Create indexes for better query performance
db.us_accidents_raw.createIndex({ ID: 1 });
db.us_accidents_cleaned.createIndex({ ID: 1 });
db.us_accidents_cleaned.createIndex({ Severity: 1 });
db.us_accidents_clustered.createIndex({ cluster: 1 });
db.kmeans_results.createIndex({ cluster_id: 1 });
db.pipeline_logs.createIndex({ timestamp: -1 });

// Create application user
db.createUser({
  user: "pipeline_user",
  pwd: "pipeline123",
  roles: [{ role: "readWrite", db: "bigdata_project" }],
});

print("MongoDB initialization completed successfully!");
