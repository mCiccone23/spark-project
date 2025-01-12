import subprocess
import os

scripts = [
    "first-question.py",
    "second-question.py",
    "third-question.py",
    "fourth-question.py",
    "fifth-question.py",
    "sixth-question.py",
    "seventh-question.py",
    "extra-analysis-1.py",
    "extra-analysis-2.py",
]

# Directory
scripts_directory = "./base_analysis"

# Esegui ogni script
for script in scripts:
    script_path = os.path.join(scripts_directory, script)
    print(f"Esecuzione di: {script_path}")

    try:
        # Esegui lo script con subprocess
        result = subprocess.run(["python3", script_path], check=True, text=True, capture_output=True)
        print(f"Output di {script}:")
        print(result.stdout)
    except subprocess.CalledProcessError as e:
        print(f"Errore durante l'esecuzione di {script}: {e}")
        print(e.stderr)

print("All scripts executed.")
