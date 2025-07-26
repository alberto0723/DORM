from tkinter import Tk
from tkinter.filedialog import askopenfilename

from .DesignTranslator import translate

if __name__ == '__main__':
    try:
        root = Tk()
        root.withdraw()
        ruta = askopenfilename(
            title="Select the model XML file.",
            filetypes=[("XML", "*.xml")]
        )
        
        translation = translate(ruta)
        
        print(translation)
        
        
    except Exception as e:
        print(f"Error generating JSON: {e}")
                