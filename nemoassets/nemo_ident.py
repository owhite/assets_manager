
import random
import db_utils

VALID_CHARS="0123456789abcdefghijkmnopqrstuvwxyz"
NUM_RANDOM_CHARS=7
NEMO_PREFIX="nemo"
PREFIX_TABLE_FIELD_MAP = {
    "dat": ("collection", "col_id"),
    "col": ("collection", "col_id"),
    "prj": ("project", "prj_id"),
    "grn": ("project", "prj_id"),
    "std": ("project", "prj_id"),
    "sbj": ("subject", "sbj_id"),
    "smp": ("sample", "smp_id"),
    "fil": ("file", "file_id"),
    "coh": ("cohort", "coh_id"),
    "inc": ("ins_cert", "ic_id"),
    "prg": ("program", "prg_id"),
    "lib": ("library", "lib_id")
}

def generate_id(prefix):
    return NEMO_PREFIX + ":" + prefix + "-" + generate_random_string(VALID_CHARS, NUM_RANDOM_CHARS)

def create_nemo_id(prefix):
    """
    Creation of an ID involves repeatedly generating IDs until a unique one is found and then
    returning it. Because the chances of a collision are very low, the code generally doesn't 
    loop at all
    """
    if prefix not in PREFIX_TABLE_FIELD_MAP:
        raise ValueError(f"Cannot create identifier for prefix '{prefix}'. It is not one of the allowable prefixes.")
    
    db = db_utils.Db()
    while True:
        nemo_id = generate_id(prefix)
        table_name, field_name = PREFIX_TABLE_FIELD_MAP[prefix]
        # Calling this 'single underscore' function from within this same pacakge 
        # so treating it as a package-level function
        result = db._get_id_from_unique_value(table_name, field_name, nemo_id)
        if not result:
            return nemo_id

def generate_random_string(alphabet, num_chars):
    """Generate a random number of alphanumeric characters based on the passed-in alphabet."""
    return ''.join(random.choice(alphabet) for i in range(num_chars))