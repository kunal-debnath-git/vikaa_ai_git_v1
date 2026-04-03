from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, EmailStr
import smtplib
from email.mime.text import MIMEText
import os
import requests
import logging

logger = logging.getLogger(__name__)
router = APIRouter()

class ContactForm(BaseModel):
    name: str
    email: EmailStr
    message: str
    math_answer: int
    math_expected: int
    hp_field: str = "" # The honeypot field

@router.post("/api/contact")
async def submit_contact(form: ContactForm):
    logger.info(f"Received contact form submission from: {form.email}")
    
    # 1️⃣ Bot Protection: Honeypot Check
    # If a bot fills this hidden field, we ignore the request but return success to trick it.
    if form.hp_field:
        logger.warning(f"Honeypot filled by bot from: {form.email}")
        return {"detail": "Message sent successfully"}

    # 2️⃣ Bot Protection: Math Challenge
    if form.math_answer != form.math_expected:
        logger.warning(f"Math validation failed for {form.email}")
        raise HTTPException(status_code=400, detail="Bot protection check failed (Incorrect math answer)")

    # 3️⃣ Send Email via SMTP (Hostinger)
    sender = os.getenv("CONTACT_EMAIL")                # e.g. contact@vikaa.ai
    receiver = os.getenv("CONTACT_EMAIL_RECEIVER")     # same or personal
    password = os.getenv("CONTACT_EMAIL_PASSWORD")     # Hostinger email password

    if not all([sender, receiver, password]):
        logger.error("Email environment variables are missing (CONTACT_EMAIL, CONTACT_EMAIL_RECEIVER, or CONTACT_EMAIL_PASSWORD)")
        raise HTTPException(status_code=500, detail="Server misconfiguration: Email credentials missing")

    body = f"Name: {form.name}\nEmail: {form.email}\n\nMessage:\n{form.message}"
    msg = MIMEText(body)
    msg["Subject"] = "New Contact Form Submission"
    msg["From"] = sender
    msg["To"] = receiver

    try:
        with smtplib.SMTP_SSL("smtp.hostinger.com", 465) as server:
            server.login(sender, password)
            server.sendmail(sender, receiver, msg.as_string())
        logger.info(f"Email sent successfully for {form.email}")
    except Exception as e:
        logger.error(f"SMTP error while sending email for {form.email}: {e}")
        raise HTTPException(status_code=500, detail=f"Email failed to send. Please try again later.")

    return {"detail": "Message sent successfully"}
