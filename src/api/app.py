import time
import os
import sys

# Dynamic path resolution to ensure nested local modules load seamlessly under Windows
script_dir = os.path.dirname(os.path.abspath(__file__))
if script_dir not in sys.path:
    sys.path.append(script_dir)

from flask import Flask, jsonify, request
from kafka_utils import KafkaInspector
from lake_utils import LakeReader

app = Flask(__name__)
inspector = KafkaInspector(bootstrap_servers="localhost:9092")
reader = LakeReader(base_path="/tmp/datalake")

@app.route("/api/v1/health", methods=["GET"])
def get_health():
    is_kafka_healthy = inspector.verify_connectivity()
    status = "healthy" if is_kafka_healthy else "degraded"
    
    return jsonify({
        "status": status,
        "timestamp": int(time.time() * 1000),
        "dependencies": {
            "kafka_cluster": "connected" if is_kafka_healthy else "disconnected",
            "storage_layer": "local_filesystem"
        }
    }), 200

@app.route("/api/v1/kafka/stats", methods=["GET"])
def get_kafka_stats():
    metadata = inspector.get_cluster_metadata(topic_name="sensor-events")
    return jsonify({
        "timestamp": int(time.time() * 1000),
        "kafka_metrics": metadata
    }), 200

@app.route("/api/v1/sensors/recent", methods=["GET"])
def get_recent_sensors():
    sensor_type = request.args.get("sensor_type", None)
    limit = request.args.get("limit", 50)
    
    try:
        records = reader.fetch_recent_records(sensor_type=sensor_type, limit=limit)
        return jsonify({
            "count": len(records),
            "data": records
        }), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/v1/sensors/summary", methods=["GET"])
def get_sensors_summary():
    sensor_type = request.args.get("sensor_type", None)
    
    try:
        summaries = reader.fetch_summary_aggregates(sensor_type=sensor_type)
        return jsonify({
            "count": len(summaries),
            "data": summaries
        }), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)