"""
Shared in-memory conversation history manager.

Imported by both LLM_LangChain and LLM_GEMINI so all modules
share a single history store per process — avoids the dual-instance
inconsistency where video-frame analysis sessions were invisible to
the main conversation thread.
"""
import threading

from langchain.memory import ConversationBufferMemory


class InMemoryHistoryManager:
    def __init__(self):
        self.sessions: dict[str, ConversationBufferMemory] = {}
        self.lock = threading.Lock()

    def get_memory(self, session_id: str) -> ConversationBufferMemory:
        with self.lock:
            if session_id not in self.sessions:
                self.sessions[session_id] = ConversationBufferMemory(
                    memory_key="chat_history", return_messages=True
                )
            return self.sessions[session_id]

    def clear_memory(self, session_id: str) -> None:
        with self.lock:
            self.sessions.pop(session_id, None)


# Single shared instance for the lifetime of the process
history_manager = InMemoryHistoryManager()
