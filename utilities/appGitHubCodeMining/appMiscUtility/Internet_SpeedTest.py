# Make sure to install speedtest-cli first:
# pip install speedtest-cli

import speedtest as st

def Speed_Test():
    import socket
    import uuid
    import requests
    import platform
    import psutil
    try:
        import GPUtil
    except ImportError:
        GPUtil = None
    test = st.Speedtest()

    down_speed = test.download()
    down_speed = round(down_speed / 10**6, 2)  # Convert to Mbps

    up_speed = test.upload()
    up_speed = round(up_speed / 10**6, 2)  # Convert to Mbps

    ping = test.results.ping

    # Get public IP and ISP info
    try:
        ip_info = requests.get('https://ipinfo.io/json', timeout=5).json()
        ip_address = ip_info.get('ip', 'N/A')
        isp = ip_info.get('org', 'N/A')
    except Exception:
        ip_address = 'N/A'
        isp = 'N/A'

    # Get MAC address
    try:
        mac_num = hex(uuid.getnode()).replace('0x', '').upper()
        mac = ':'.join(mac_num[i:i+2] for i in range(0, 12, 2))
    except Exception:
        mac = 'N/A'

    # Get system info
    sys_info = []
    sys_info.append(f"System: {platform.system()} {platform.release()} ({platform.version()})")
    sys_info.append(f"Machine: {platform.machine()}")
    sys_info.append(f"Processor: {platform.processor()}")
    sys_info.append(f"CPU Cores: {psutil.cpu_count(logical=False)} Physical / {psutil.cpu_count(logical=True)} Logical")
    sys_info.append(f"Memory: {round(psutil.virtual_memory().total / (1024**3), 2)} GB")

    # GPU info (if available)
    gpu_info = "No GPU detected"
    if GPUtil:
        gpus = GPUtil.getGPUs()
        if gpus:
            gpu_info = ", ".join([f"{gpu.name} ({gpu.memoryTotal}MB)" for gpu in gpus])
    sys_info.append(f"GPU: {gpu_info}")

    # Prepare message
    msg = (
        f"Download Speed: {down_speed} Mbps\n"
        f"Upload Speed: {up_speed} Mbps\n"
        f"Ping: {ping} ms\n"
        f"====================================\n"        
        f"IP Address: {ip_address}\n"
        f"MAC Address: {mac}\n"
        f"ISP: {isp}\n"
        f"====================================\n"
        + "\n".join(sys_info)
    )

    # Show popup using tkinter
    try:
        import tkinter as tk
        from tkinter import messagebox
        root = tk.Tk()
        root.withdraw()  # Hide main window
        messagebox.showinfo("Internet Speed Test Result", msg)
        root.destroy()
    except Exception as e:
        print("Could not display popup. Output:\n", msg)
        print("Error:", e)

# Run the test
def main():
    Speed_Test()

if __name__ == "__main__":
    main()



# /// Oritinal
# def Speed_Test():
#     test = st.Speedtest()

#     down_speed = test.download()
#     down_speed = round(down_speed / 10**6, 2)  # Convert to Mbps
#     print("Download Speed in Mbps: ", down_speed)

#     up_speed = test.upload()
#     up_speed = round(up_speed / 10**6, 2)  # Convert to Mbps
#     print("Upload Speed in Mbps: ", up_speed)

#     ping = test.results.ping
#     print("Ping: ", ping)

# # Run the test
# Speed_Test()