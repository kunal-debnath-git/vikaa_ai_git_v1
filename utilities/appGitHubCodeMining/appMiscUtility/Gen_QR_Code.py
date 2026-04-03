# pip install qrcode
# pip install Image

import qrcode
from PIL import Image

data = input("Enter anything to generate QR: ")
qr = qrcode.QRCode(version=3, box_size=8, border=4)
qr.add_data(data)
qr.make(fit=True)

imageGreenYellow = qr.make_image(fill="Black", back_color="GreenYellow")
imageGreenYellow.save("qr_codeGreenYellow.png")
Image.open("qr_codeGreenYellow.png")

imageBlackWhite = qr.make_image(fill="Black", back_color="White")
imageBlackWhite.save("qr_codeBlackWhite.png")
Image.open("qr_codeBlackWhite.png")
