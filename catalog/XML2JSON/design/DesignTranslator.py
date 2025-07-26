from .txSerialization import TxSerialization
from .txParsing import TxParsing

def translate(root: str) -> str:
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
                    