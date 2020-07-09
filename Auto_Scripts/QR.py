##method-1

import qrcode
qr=qrcode.make("how is it going")
qr.save("sample.png")

##method-2

qr=qrcode.QRCode(version=1,box_size=15,border=2)
data=("www.google.com")
qr.add_data(data)
qr.make(fit=True)
img=qr.make_image(fill='red',back_color='black')
img.save("col.png")
