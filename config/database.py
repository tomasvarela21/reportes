"""
Configuración de conexión a la base de datos Neon PostgreSQL
"""
import os
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

class DatabaseConfig:
    """Configuración de la base de datos"""
    
    def __init__(self):
        self.database_url = os.getenv('DATABASE_URL')
        if not self.database_url:
            raise ValueError("DATABASE_URL no está configurado en el archivo .env")
        
        # Crear engine con pool de conexiones
        self.engine = create_engine(
            self.database_url,
            poolclass=NullPool,  # Neon maneja el pooling
            echo=os.getenv('DEBUG', 'False') == 'True'
        )
        
        # Crear session maker
        self.SessionLocal = sessionmaker(
            autocommit=False,
            autoflush=False,
            bind=self.engine
        )
    
    def get_session(self):
        """Obtener una nueva sesión de base de datos"""
        return self.SessionLocal()
    
    def test_connection(self):
        """Probar la conexión a la base de datos"""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text("SELECT 1"))
                return True
        except Exception as e:
            print(f"Error al conectar a la base de datos: {e}")
            return False
    
    def execute_sql_file(self, filepath):
        """Ejecutar un archivo SQL"""
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                sql = f.read()
            
            with self.engine.connect() as conn:
                # Ejecutar cada statement por separado
                statements = sql.split(';')
                for statement in statements:
                    if statement.strip():
                        conn.execute(text(statement))
                        conn.commit()
            
            return True, "SQL ejecutado correctamente"
        except Exception as e:
            return False, f"Error ejecutando SQL: {str(e)}"

# Instancia global
db_config = DatabaseConfig()