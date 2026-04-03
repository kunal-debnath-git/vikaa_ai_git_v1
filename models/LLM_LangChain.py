# # File: models/LLM_LangChain_MCP.py
# pip install python-docx openpyxl
# pip install python-pptx
# pip install moviepy
# pip install openai-whisper
# pip install yt-dlp
# pip install soundfile
# ===============================================
import os
import threading
import xml.etree.ElementTree as ET
from types import SimpleNamespace
from base64 import b64decode

from langchain.memory import ConversationBufferMemory
# from langchain_openai import ChatOpenAI
# from langchain_google_genai import GoogleGenerativeAIEmbeddings
import re
from models.attachment_handlers import *
from models.LLM_GEMINI import *

from dotenv import load_dotenv
load_dotenv()
# ==================================================================
GEMINI_KEY = os.getenv("GEMINI_API_KEY")
OPENAI_KEY = os.getenv("OPENAI_API_KEY")

MAX_COMBINED_ATTACHMENT_MB = 20
# ==================================================================
class InMemoryHistoryManager:
    def __init__(self):
        self.sessions = {}
        self.lock = threading.Lock()

    def get_memory(self, session_id):
        with self.lock:
            if session_id not in self.sessions:
                self.sessions[session_id] = ConversationBufferMemory(memory_key="chat_history", return_messages=True)
            return self.sessions[session_id]

    def clear_memory(self, session_id):
        with self.lock:
            if session_id in self.sessions:
                del self.sessions[session_id]

history_manager = InMemoryHistoryManager()
# ==================================================================
def sanitize_session_id(session_id):
    return ''.join(c for c in session_id if c.isalnum() or c in ('-', '_'))

# ==================================================================

def invoke_langchain(agent_request):
    try:
        session_id = sanitize_session_id(agent_request.session_id)
        model_choice = agent_request.model.lower()
        temperature = getattr(agent_request, "temperature", 0.6)
        query = agent_request.query
        attachments = getattr(agent_request, "attachments", [])
        memory = history_manager.get_memory(session_id)

        use_vision = False
        image_payloads = []
        attachment_texts = []
        total_attachment_bytes = 0

        # ✅ Check for YouTube URL in the query BEFORE processing attachments
        print(f">> Checking YouTube/ETC link in query: {query}")
        yt_match = re.search(r'(https?://(?:www\.)?youtube\.com/watch\?v=[\w-]+|https?://youtu\.be/[\w-]+)', query)
        if yt_match:
            youtube_url = yt_match.group(1)
            print(f">> Matched YouTube URL: {youtube_url}")
            return handle_youtube_link(youtube_url, query)

        # ✅ Process file attachments
        for att in attachments:
            base64_data = att.dataUrl.split(",")[1]
            decoded_bytes = b64decode(base64_data)
            total_attachment_bytes += len(decoded_bytes)

            filename = att.filename.lower()

            print(f">> Received attachment: {att.filename}")
            print(f">> Raw dataUrl starts with: {att.dataUrl[:50]}")
            print(f">> Query: {query}")

            if att.dataUrl.startswith("data:image"):
                use_vision = True
                image_type = att.dataUrl.split(";")[0].replace("data:", "")  # extract MIME type
                image_payloads.append({ "mime_type": image_type, "data": base64_data })
            elif filename.endswith(".pdf"):
                attachment_texts.append(handle_pdf(att.filename, decoded_bytes))
            # ===============================================================================                
            elif filename.endswith(".csv"):
                attachment_texts.append(handle_csv(att.filename, decoded_bytes))            
            elif filename.endswith(".xlsx"):
                attachment_texts.append(handle_xlsx(att.filename, decoded_bytes))
            elif filename.endswith(".json"):
                attachment_texts.append(handle_json(att.filename, decoded_bytes))
            # ===============================================================================                
            elif filename.endswith(".zip") or filename.endswith(".tar") or filename.endswith(".gz"):
                attachment_texts.append(handle_archive(att.filename, decoded_bytes))
            # ===============================================================================
            elif filename.endswith(".docx"):
                attachment_texts.append(handle_docx(att.filename, decoded_bytes))
            elif filename.endswith(".xml"):
                attachment_texts.append(handle_xml(att.filename, decoded_bytes))                
            elif filename.endswith(".pptx"):
                attachment_texts.append(handle_pptx(att.filename, decoded_bytes))                
            # ===============================================================================
            elif filename.endswith(".doc"):
                attachment_texts.append(f"\n⚠️ Legacy .doc format not supported for parsing: {att.filename}. Please convert to .docx")
            elif filename.endswith(".xls"):
                attachment_texts.append(f"\n⚠️ Legacy .xls format not supported for parsing: {att.filename}. Please convert to .xlsx")
            elif filename.endswith(".ppt"):
                attachment_texts.append(f"\n⚠️ Legacy .ppt format not supported for parsing: {att.filename}. Please convert to .pptx")
            elif filename.endswith(".pcx") or filename.endswith(".pbrush"):
                attachment_texts.append(f"\n⚠️ Unsupported legacy Paintbrush format: {att.filename}")
            # ===============================================================================
            elif filename.endswith(".mp3") or filename.endswith(".wav") or filename.endswith(".m4a") or filename.endswith(".flac"):
                # attachment_texts.append(handle_audio(att.filename, decoded_bytes))
                attachment_texts.append(route_audio_handler(att.filename, decoded_bytes, query))

            elif filename.endswith(".mp4") or filename.endswith(".mov") or filename.endswith(".webm"):
                attachment_texts.append(handle_video(att.filename, decoded_bytes))
            else:
                attachment_texts.append(f"\n📎 File attached: {att.filename} (unprocessed format)")

        if total_attachment_bytes > (MAX_COMBINED_ATTACHMENT_MB * 1024 * 1024):
            return f"⚠️ You have exceeded the maximum combined attachment size of {MAX_COMBINED_ATTACHMENT_MB}MB."

        ##############################################################################
        if model_choice == "gpt":
            return "[Placeholder] GPT logic not implemented yet."
        elif model_choice == "gemini":
            return handle_gemini(query, use_vision, image_payloads, attachment_texts, memory, temperature)
        else:
            return f"⚠️ Unsupported model '{model_choice}'"
        ##############################################################################
        
    except Exception as e:
        return f"Model error from **invoke_langchain()/Exception**: {e}"

# ==================================================================

if __name__ == "__main__":
    import os
    from dotenv import load_dotenv
    from types import SimpleNamespace
    from base64 import b64encode
    import mimetypes
    load_dotenv()

    GEMINI_KEY = os.getenv("GEMINI_API_KEY")
    OPENAI_KEY = os.getenv("OPENAI_API_KEY")

    print("\n=== Testing Gemini Model Conversation ===")
    session_id = "test_session_456"

    # 📎 Simulate uploading a file (image/pdf/csv)
    file_path = "sample.pdf"  # ✅ Change to your test file name
    user_input = "Hi there!, Please analyze this attached file."
    
    # file_path = "D:\MyDownloads\Low-Res_EN-Slide2.jpg"  
    # user_input = "Hi there!, Please analyze this attached file."
    
    # file_path = "D:\_ _GenAI-and-ML\Datafiles\MiscDataset\sales-data-sample.csv" 
    # user_input = "What insights can you draw from this sales dataset?"
    # file_path = "D:\__Azure Training(Office)\DataBricks-Workshop-11Aug2021\Azure Databricks Hands-on Lab Guide.pdf" 
    # # user_input = "Summarize my resume and identify skills listed." 

    # file_path = "D:\MyDownloads\LLM_Flow_Send_to_Response.docx" 
    # user_input = "Summarize the main points of this business proposal."

    # file_path = "D:\__AZURE\__Project_Azure_Stream\TelcoEventData.json"
    # user_input = "Please describe the data schema and key-value structure in this file."

    # file_path = "E:\AgentWithMCP_v1\datastore\PurchaseOrders.xml"
    # user_input = "Explain the structure and list top-level elements in this XML file."

    # file_path = "D:\__AZURE\PowerBI-ALL\PowerBI ALL.pptx"
    # user_input = "Extract the key messages from this company presentation."

    # file_path = "D:\MyDownloads\Car-Lease-vs-Buying.xlsx"
    # user_input = "What kind of financial data is captured here?"

    # file_path = "D:\MyDownloads\AzureFunctionStarterProject.zip"
    # user_input = "List the contents of this archive and describe what type of files it contains."
    
    # ==================================================================
    # # ✅ Update path to your test video
    # file_path = r"E:\Training Materials\Azure Udemy Training\Udemy - AZ-300 Azure Architecture Technologies Certification Exam\33. 70-535 2018 Edition - Hybrid Applications\1. Introduction to Hybrid Applications.mp4"
    # file_path = r"E:\Training Materials\Azure Udemy Training\Udemy - AZ-300 Azure Architecture Technologies Certification Exam\59. Wrapping Up and Errata\1. Thank You!.mp4"
    # user_input = "summarize this video in 10 line"

    # # ✅ Update path to your test audio
    file_path = r"E:\__Test_Files_For_AgenticAI\audio-video\11 Adele - Someone Like You.mp3"
    file_path = r"E:\__Test_Files_For_AgenticAI\audio-video\Jai Ho.mp3"
    user_input = "I want to know lyrics and chords from this song"
    
    # ==================================================================
    file_name = os.path.basename(file_path)
    mime_type, _ = mimetypes.guess_type(file_path)

    with open(file_path, "rb") as f:
        encoded = b64encode(f.read()).decode("utf-8")
        data_url = f"data:{mime_type};base64,{encoded}"

    from types import SimpleNamespace
    attachment = SimpleNamespace(
        filename=file_name,
        dataUrl=data_url
    )

    agent_request = SimpleNamespace(
        session_id=session_id,
        query=user_input,
        model="gemini",
        attachments=[attachment]
    )

    response = invoke_langchain(agent_request)
    print("\n🧠 LLM Response:\n", response)
    print("\n✅ Testing completed.")

# =====================================================
# for YOUTUE ONLY
# =====================================================
# if __name__ == "__main__":
#     import os
#     import re
#     from dotenv import load_dotenv
#     from types import SimpleNamespace
#     load_dotenv()
#     GEMINI_KEY = os.getenv("GEMINI_API_KEY")
#     OPENAI_KEY = os.getenv("OPENAI_API_KEY")

#     print("\n=== Testing Gemini Model with YouTube Link ===")
#     session_id = "test_session_youtube"
#     user_input = "May I know lyrics of this YouTube video: https://www.youtube.com/watch?v=jUrKa6thMCU "
#     # user_input = "May I know lyrics of this Hindi YouTube video: https://www.youtube.com/watch?v=qgDTT2E3lSQ "
#     # user_input = "May I know lyrics of this Bengali YouTube video: https://www.youtube.com/watch?v=DGc0o4YI6xk "
    
#     # 🧪 Create request with no attachments — only a query containing a YouTube URL
#     agent_request = SimpleNamespace(
#         session_id=session_id,
#         query=user_input,
#         model="gemini",
#         attachments=[]
#     )
#     response = invoke_langchain(agent_request)
#     print("\n🧠 LLM Response:\n", response)
#     print("\n✅ YouTube test completed.")
