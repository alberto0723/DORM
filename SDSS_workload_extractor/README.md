# 📊 Database Workload Extraction and Analysis

This repository contains the implementation of an automated workload extraction and analysis tool designed to retrieve, process, and analyze SQL workload data from the SkyServer database.

## 🔍 Project Information

- **🎓 Thesis Title:** Database Workload Extraction and Analysis  
- **👩‍💻 Author:** Rozhina Ahmadi  
- **🧑‍🏫 Supervisor:** Alberto Abelló Gamazo  
- **👩‍🏫 Co-Supervisor:** Wafaa Radwan

--- 

## 🚀 Getting Started

### 1. 📥 Clone the Repository

Open your terminal or PowerShell and run:

```bash
git clone https://github.com/rozhinaahmadii/workload-automation.git
```
```bash
cd workload-automation/backend
```
## 💻 Backend Setup
### 2. 🐍 Set Up the Virtual Environment

Create a Python virtual environment: 
```bash
python -m venv env
```
Activate it: 
- **Windows:** 
```bash
.\env\Scripts\Activate.ps1
```

- **macOS/Linux:** 
```bash
source env/bin/activate
```
### 3. 📦 Install Dependencies

With the virtual environment activated, install all required packages:

```bash
pip install -r requirements.txt
```

To update the list of dependencies later, run:

```bash
pip freeze > requirements.txt
```
### 4. ▶️ Using the Command-Line Interface(CLI)

Start the server with:Run any CLI operation directly from the backend folder:

```bash
cd backend
python main.py [command] [options]
```
| Command  | Description                        |
| -------- | -----------------------------------|
| `count`  | Count total logs from SDSS         |
| `fetch`  | Download logs for a month/year     |
| `parse`  | Extract query structure            |
| `clean`  | Remove MyDB/private queries        |
| `group`  | Group similar queries by structure |

🗂️ All output is automatically saved inside the backend/data/ folder.


### 5. ▶️ Run the FastAPI Server
To run the backend as an API:

```bash
uvicorn main_api:app --reload
```
- **Root Endpoint:** [http://127.0.0.1:8000](http://127.0.0.1:8000)

- **Swagger UI (Interactive Docs):**  
  [http://127.0.0.1:8000/docs](http://127.0.0.1:8000/docs)

- **ReDoc (Alternative Docs):**  
  [http://127.0.0.1:8000/redoc](http://127.0.0.1:8000/redoc)

---

## 💻 Frontend Setup

The frontend is built with **React + TypeScript** using **Vite**. It provides an interactive interface to upload files, run analysis, view statistics, and visualize query workloads.

### 1 📁 Navigate to the Frontend Folder, install and start the development server

```bash
cd frontend
npm install
npm run dev
```
📝 Note: Make sure the backend server is running before launching the frontend.

---

## 🛑 .gitignore Highlights

The following folders and files are ignored:

- `backend/env/` → virtual environment
- `__pycache__/`, `*.pyc` → Python cache
- `.vscode/`, `.idea/` → IDE settings
- `logs/`, `*.log`, `*.csv` → Log and csv files
- `data/, backend/data/` → Parsed output or temporary data
- `frontend/node_modules/` → Frontend dependencies
- `DS_Store, Thumbs.db` → OS-generated system files

## Manuals
The `docs` folder contains the user manuals for this project. These manuals were written in Markdown to enable clean structure, syntax-highlighted code blocks, and professional academic formatting. Pandoc was used to convert the Markdown files into docs format, allowing seamless integration into the main project documentation. 

Example: 
```bash
pandoc cli_manual.md -o cli_manual.docx
pandoc frontend_manual.md -o frontend_manual.docx
pandoc cli_manual.md -o cli_manual.pdf --pdf-engine=pdflatex
```