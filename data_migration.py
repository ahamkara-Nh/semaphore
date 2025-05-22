import json
import os
import sqlite3
from database import get_db_connection

def load_json_file(file_path):
    """Load and parse a JSON file."""
    with open(file_path, 'r', encoding='utf-8') as f:
        return json.load(f)

def migrate_product_data():
    """Migrate product data from JSON files to SQLite database."""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Get all JSON files from replacement directory
    replacement_dir = os.path.join(os.path.dirname(__file__), 'replacement')
    json_files = [f for f in os.listdir(replacement_dir) if f.endswith('.json')]
    
    try:
        # First pass: Create categories
        for json_file in json_files:
            category_name = os.path.splitext(json_file)[0]
            cursor.execute(
                "INSERT OR IGNORE INTO product_category (name) VALUES (?)",
                (category_name,)
            )
        
        # Second pass: Create products
        for json_file in json_files:
            file_path = os.path.join(replacement_dir, json_file)
            category_name = os.path.splitext(json_file)[0]
            
            # Get category_id
            cursor.execute(
                "SELECT category_id FROM product_category WHERE name = ?",
                (category_name,)
            )
            category_id = cursor.fetchone()[0]
            
            # Load and process products
            data = load_json_file(file_path)
            for product in data.get('food', []):
                for serve in product.get('serves', []):
                    fodmap = serve.get('fodmap', [])
                    if len(fodmap) >= 9:  # Ensure we have enough FODMAP values
                        # Process allergies
                        allergies = product.get('allergy', [])
                        contains_nuts = 0 in allergies
                        contains_peanut = 1 in allergies
                        contains_gluten = 2 in allergies
                        contains_eggs = 3 in allergies
                        contains_fish = 4 in allergies
                        contains_soy = 5 in allergies

                        # Get replacement name if available
                        replacements = product.get('replacement', [])
                        replacement_name = replacements[0].get('name', None) if replacements else None

                        cursor.execute("""
                            INSERT INTO product (
                                name, category_id, fructose_level, lactose_level,
                                fructan_level, mannitol_level, sorbitol_level, gos_level,
                                serving_title, serving_amount_grams,
                                contains_nuts, contains_peanut, contains_gluten,
                                contains_eggs, contains_fish, contains_soy,
                                replacement_name
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, (
                            product['name'],
                            category_id,
                            fodmap[0],  # fructose
                            fodmap[1],  # lactose
                            fodmap[4],  # fructan
                            fodmap[3],  # mannitol
                            fodmap[2],  # sorbitol
                            fodmap[5],  # gos
                            serve['title'],
                            fodmap[8],   # serving amount in grams
                            contains_nuts,
                            contains_peanut,
                            contains_gluten,
                            contains_eggs,
                            contains_fish,
                            contains_soy,
                            replacement_name
                        ))
        
        conn.commit()
        print("Data migration completed successfully!")
        
    except Exception as e:
        conn.rollback()
        print(f"Error during migration: {e}")
        raise
    
    finally:
        conn.close()

if __name__ == '__main__':
    migrate_product_data()