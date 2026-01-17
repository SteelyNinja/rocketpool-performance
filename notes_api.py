#!/usr/bin/env python3
"""
Simple Notes API for Rocket Pool Dashboard
Handles saving and loading notes for node addresses.
"""

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import json
import os
import shutil
from typing import Dict, Any
from datetime import datetime
import uvicorn

app = FastAPI(title="Rocket Pool Notes API", version="1.0.0")

# CORS - Allow requests from rocketpool.steely-test.org
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://rocketpool.steely-test.org",
        "http://rocketpool.steely-test.org",
        "http://localhost:3000",  # For local development
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

NOTES_FILE = '/home/gary/rocketpool/reports/notes.json'
BACKUP_DIR = '/home/gary/rocketpool/reports/backup'

class NotesData(BaseModel):
    notes: Dict[str, Any]

def backup_notes():
    """Create a timestamped backup of the current notes file"""
    try:
        # Create backup directory if it doesn't exist
        os.makedirs(BACKUP_DIR, exist_ok=True)

        # Only backup if notes file exists
        if os.path.exists(NOTES_FILE):
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            backup_file = os.path.join(BACKUP_DIR, f'notes_backup_{timestamp}.json')
            shutil.copy2(NOTES_FILE, backup_file)
            print(f"✓ Backup created: {backup_file}")
            return backup_file
        else:
            print("No notes file to backup")
            return None
    except Exception as e:
        print(f"Warning: Failed to create backup: {e}")
        # Don't fail the save operation if backup fails
        return None

@app.get("/api/rp-notes")
async def get_notes():
    """Retrieve all notes"""
    try:
        if os.path.exists(NOTES_FILE):
            with open(NOTES_FILE, 'r') as f:
                data = json.load(f)
                return {"success": True, "notes": data}
        return {"success": True, "notes": {}}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error reading notes: {str(e)}")

@app.post("/api/rp-notes")
async def save_notes(data: NotesData, request: Request):
    """Save all notes (with automatic backup)"""
    try:
        # Create backup before saving
        backup_file = backup_notes()

        # Ensure directory exists
        os.makedirs(os.path.dirname(NOTES_FILE), exist_ok=True)

        # Write notes to file
        with open(NOTES_FILE, 'w') as f:
            json.dump(data.notes, f, indent=2)

        # Log the operation
        client_ip = request.client.host
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(f"[{timestamp}] Notes saved by {client_ip} | Backup: {os.path.basename(backup_file) if backup_file else 'none'}")

        return {
            "success": True,
            "message": "Notes saved successfully",
            "backup": os.path.basename(backup_file) if backup_file else None
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error saving notes: {str(e)}")

@app.get("/api/rp-notes/backups")
async def list_backups():
    """List all backup files"""
    try:
        if not os.path.exists(BACKUP_DIR):
            return {"success": True, "backups": [], "count": 0}

        backups = []
        for filename in sorted(os.listdir(BACKUP_DIR), reverse=True):
            if filename.startswith('notes_backup_') and filename.endswith('.json'):
                filepath = os.path.join(BACKUP_DIR, filename)
                stat = os.stat(filepath)
                backups.append({
                    "filename": filename,
                    "size": stat.st_size,
                    "created": datetime.fromtimestamp(stat.st_mtime).strftime('%Y-%m-%d %H:%M:%S')
                })

        return {
            "success": True,
            "backups": backups,
            "count": len(backups),
            "backup_dir": BACKUP_DIR
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error listing backups: {str(e)}")

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "service": "rocketpool-notes-api"}

if __name__ == "__main__":
    # Create empty notes file if it doesn't exist
    if not os.path.exists(NOTES_FILE):
        os.makedirs(os.path.dirname(NOTES_FILE), exist_ok=True)
        with open(NOTES_FILE, 'w') as f:
            json.dump({}, f)

    uvicorn.run(app, host="127.0.0.1", port=8001)
