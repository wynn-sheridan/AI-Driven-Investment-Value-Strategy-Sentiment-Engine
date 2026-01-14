import vnstock
import inspect
from vnstock import Screener

print("--- AVAILABLE MODULES IN VNSTOCK ---")
# This lists all functions you can import
print(dir(vnstock)) 

print(Screener.__module__)

# This checks if the 'Trading' module (for VCI) exists
try:
    from vnstock.explorer.vci.trading import Trading
    print("\n✅ SUCCESS: 'Trading' module (VCI Source) is available.")
except ImportError:
    print("\n❌ FAILURE: 'Trading' module not found.")