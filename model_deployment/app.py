from flask import Flask, request, jsonify
from logger import logger
from dbs import init_db, init_redis_db, insert_sqlite, check_product_id_cache

# Initialize Flask app
app = Flask(__name__)

# Recommended products (static set)
recommended_product_ids = [1, 2, 3]

# API endpoint
@app.route("/recommend", methods=["POST"])
def recommend():
    # Get product_id from request
    data = request.json
    product_id = data.get("product_id")

    logger.info(f"Request received: product_id={product_id}")

    if not product_id:
        return jsonify({"error": "product_id is required"}), 400

    check_product_id_cache(product_id=product_id, recommended_product_ids=recommended_product_ids)
    insert_sqlite(product_id=product_id, recommended_product_ids=recommended_product_ids)
    logger.info(f"Request received: product_id={product_id}, recommended_products={recommended_product_ids}")


    # Return response
    return jsonify({"product_id": product_id, "recommended_products": recommended_product_ids})

if __name__ == "__main__":
    init_db()
    init_redis_db()
    app.run(host="0.0.0.0", port=5000)
