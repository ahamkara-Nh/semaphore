import sqlite3
import json
from typing import Dict, Any, List
from datetime import datetime

def validate_recipe_json(recipe_data: Dict[str, Any]) -> tuple[bool, str]:
    """
    Validates if the recipe JSON has all required fields and correct data types.
    Returns a tuple of (is_valid: bool, error_message: str)
    """
    required_fields = {
        'name': str,
        'image_name': str,
        'ingredients': (list, str),  # Can be either list or string
        'instructions': (list, str)  # Can be either list or string
    }
    
    # Check for required fields
    for field, expected_type in required_fields.items():
        if field not in recipe_data:
            return False, f"Missing required field: {field}"
        
        # Handle fields that can be either list or string
        if isinstance(expected_type, tuple):
            if not isinstance(recipe_data[field], expected_type):
                return False, f"Field {field} must be one of types: {expected_type}"
        else:
            if not isinstance(recipe_data[field], expected_type):
                return False, f"Field {field} must be of type: {expected_type}"
    
    return True, ""

def format_recipe_data(recipe_data: Dict[str, Any]) -> Dict[str, str]:
    """
    Formats the recipe data to match the database schema.
    Converts lists to newline-separated strings if necessary.
    """
    formatted_data = recipe_data.copy()
    
    # Convert ingredients list to string if it's a list
    if isinstance(formatted_data['ingredients'], list):
        formatted_data['ingredients'] = '\n'.join(formatted_data['ingredients'])
    
    # Convert instructions list to string if it's a list
    if isinstance(formatted_data['instructions'], list):
        # Add numbers to instructions if they don't have them
        formatted_instructions = []
        for i, instruction in enumerate(formatted_data['instructions'], 1):
            if not instruction.strip().startswith(str(i)):
                instruction = f"{i}. {instruction}"
            formatted_instructions.append(instruction)
        formatted_data['instructions'] = '\n'.join(formatted_instructions)
    
    # Rename 'instructions' to 'preparation' to match DB schema
    formatted_data['preparation'] = formatted_data.pop('instructions')
    
    return formatted_data

def insert_recipe(recipe_json: str, db_path: str = './test.db') -> tuple[bool, str]:
    """
    Inserts a recipe into the database from a JSON string.
    Returns a tuple of (success: bool, message: str)
    """
    try:
        # Parse JSON
        recipe_data = json.loads(recipe_json) if isinstance(recipe_json, str) else recipe_json
        
        # Validate recipe data
        is_valid, error_message = validate_recipe_json(recipe_data)
        if not is_valid:
            return False, error_message
        
        # Format the data
        formatted_data = format_recipe_data(recipe_data)
        
        # Insert into database
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT INTO recipes (name, image_name, ingredients, preparation)
            VALUES (?, ?, ?, ?)
        ''', (
            formatted_data['name'],
            formatted_data['image_name'],
            formatted_data['ingredients'],
            formatted_data['preparation']
        ))
        
        conn.commit()
        conn.close()
        
        return True, "Recipe inserted successfully!"
        
    except json.JSONDecodeError:
        return False, "Invalid JSON format"
    except sqlite3.Error as e:
        return False, f"Database error: {str(e)}"
    except Exception as e:
        return False, f"Error: {str(e)}"

def get_all_recipes(db_path: str = './test.db') -> tuple[bool, List[Dict[str, Any]] | str]:
    """
    Retrieves all recipes from the database.
    Returns a tuple of (success: bool, data: List[Dict] | error_message: str)
    """
    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row  # This enables column access by name
        cursor = conn.cursor()
        
        cursor.execute('SELECT * FROM recipes ORDER BY created_at DESC')
        rows = cursor.fetchall()
        
        # Convert rows to list of dictionaries
        recipes = []
        for row in rows:
            recipe = {
                'recipe_id': row['recipe_id'],
                'name': row['name'],
                'image_name': row['image_name'],
                'ingredients': row['ingredients'].split('\n') if row['ingredients'] else [],
                'preparation': row['preparation'].split('\n') if row['preparation'] else [],
                'created_at': row['created_at'],
                'updated_at': row['updated_at']
            }
            recipes.append(recipe)
        
        conn.close()
        return True, recipes
        
    except sqlite3.Error as e:
        return False, f"Database error: {str(e)}"
    except Exception as e:
        return False, f"Error: {str(e)}"

def main():
    # Example usage for inserting a recipe
    example_recipe = {
        "name": "Салат с кальмаром и перцем",
        "image_name": "calmar_salad",
        "ingredients": [
            "Кальмар кольцами или соломкой — 200 г",
            "Перец красный сладкий — 150 г",
            "Огурец свежий — 150 г",
            "Яйцо — 3 шт.",
            "Майонез — 2 ст. л. на порцию",
            "Соль, перец — По вкусу"
        ],
        "instructions": [
            "Кальмара и яйца отварить.",
            "Перец, огурец, кальмар и яйца нарезать тонкой соломкой.",
            "Заправить майонезом, посолить, поперчить, перемешать."
        ]
    }
    
    # Insert the recipe
    success, message = insert_recipe(example_recipe)
    print("Insertion result:", message)
    
    # Retrieve and display all recipes
    success, result = get_all_recipes()
    if success:
        print("\nAll recipes in database:")
        for recipe in result:
            print(f"\nRecipe: {recipe['name']}")
            print(f"Image: {recipe['image_name']}")
            print("\nIngredients:")
            for ingredient in recipe['ingredients']:
                print(f"- {ingredient}")
            print("\nPreparation:")
            for step in recipe['preparation']:
                print(step)
            print(f"\nCreated at: {recipe['created_at']}")
            print("-" * 50)
    else:
        print(f"Error retrieving recipes: {result}")

if __name__ == "__main__":
    main() 