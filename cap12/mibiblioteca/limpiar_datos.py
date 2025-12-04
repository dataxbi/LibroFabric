def limpiar_texto(str): 
    """
    Limpia el texto de caracteres no deseados y espacios en blanco.
    
    Args:
        str (str): El texto a limpiar.
        
    Returns:
        str: El texto limpio.
    """
    import re
    
    # Eliminar caracteres no alfanuméricos (excepto espacios)
    texto_limpio = re.sub(r'[^a-zA-Z0-9\s]', '', str)
    
    # Eliminar espacios en blanco adicionales
    texto_limpio = ' '.join(texto_limpio.split())
    
    return texto_limpio