from src.db.es_connection import ESConnection
from pathlib import Path
import sys

ROOT = Path.cwd() if (Path.cwd() / "data").exists() else Path.cwd().parent
sys.path.insert(0, str(ROOT / "src"))
es_conn = ESConnection()

interventions = es_conn.get_interventions_containing_word("immigration")

print(len(interventions))
