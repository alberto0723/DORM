from pathlib import Path
import traceback
from .txSerialization import TxSerialization
from .txParsing import TxParsing

def translate(root: Path) -> str:
    try:
        lector = TxParsing()
        lector.loadComponents(root)
        
        escriptor = TxSerialization()
        escriptor.setComponents(lector.getComponents())
        escriptor.setMapComponents(lector.getMapComponents())
        escriptor.setDomainReference(lector.getDomainReference())

        return escriptor.createJSON()
    except Exception as e:
        print(f"Error generating JSON: {e}")
        traceback.print_exc()
                    