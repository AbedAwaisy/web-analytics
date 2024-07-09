import React, { useState, useEffect } from 'react';
import { saveNote, fetchNotes, updateNote, deleteNote } from '../../api/api';
import './MyNotes.css';

const MyNotes = () => {
  const [notes, setNotes] = useState([]);
  const [newNote, setNewNote] = useState('');
  const [editNoteId, setEditNoteId] = useState(null);
  const [error, setError] = useState('');

  useEffect(() => {
    fetchNotes(setError).then(data => {
      if (data) {
        setNotes(data);
      }
    });
  }, []);

  const handleSaveNote = () => {
    if (newNote.trim() === '') {
      return;
    }

    if (editNoteId !== null) {
      updateNote(editNoteId, newNote, setError).then(data => {
        if (data) {
          setNotes(notes.map(note => note.id === editNoteId ? { ...note, note: newNote } : note));
          setNewNote('');
          setEditNoteId(null);
          setError('Note updated successfully');
        }
      });
    } else {
      saveNote(newNote, setError).then(data => {
        if (data) {
          setNotes([...notes, { id: data.id, note: newNote }]);
          setNewNote('');
          setError('Note saved successfully');
        }
      });
    }
  };

  const handleEditNote = (id, note) => {
    setEditNoteId(id);
    setNewNote(note);
  };

  const handleDeleteNote = (id) => {
    deleteNote(id, setError).then(data => {
      if (data) {
        setNotes(notes.filter(note => note.id !== id));
        setError('Note deleted successfully');
      }
    });
  };

  return (
    <div className="notes-container">
      <h2> Write your Notes:</h2>
      {error && <p className="error">{error}</p>}
      <textarea
        className="note-input"
        value={newNote}
        onChange={(e) => setNewNote(e.target.value)}
        placeholder="Write your note here..."
      ></textarea>
      <button className="save-note-button" onClick={handleSaveNote}>
        {editNoteId !== null ? 'Update Note' : 'Save Note'}
      </button>
      <div className="saved-notes">
        <h2 className="my_notes"> My Notes:</h2>
        {notes.map(note => (
          <div key={note.id} className="note">
            <div className="note-content">{note.note}</div>
            <div className="note-buttons">
              <button className="edit-note-button" onClick={() => handleEditNote(note.id, note.note)}>Edit</button>
              <button className="delete-note-button" onClick={() => handleDeleteNote(note.id)}>Delete</button>
            </div>
          </div>
        ))}
      </div>
    </div>
  );
};

export default MyNotes;
