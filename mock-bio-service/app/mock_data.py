def assembly_fasta() -> bytes:
    return b">mock_contig_1\nATCGATCGATCGATCGATCGATCG\n"


def annotation_json() -> dict:
    return {
        "genes": [
            {
                "expert": "mock-gene-1",
                "start": 1,
                "stop": 100,
                "nt": "ATCGATCG",
                "aa": "MKKLL",
            },
            {
                "expert": "mock-gene-2",
                "start": 150,
                "stop": 300,
                "nt": "GCTAGCTA",
                "aa": "MALW",
            },
        ]
    }


def prediction_csv() -> bytes:
    return (
        b"antibiotic,prediction\n"
        b"ampicillin,resistant\n"
        b"ciprofloxacin,sensitive\n"
        b"tetracycline,resistant\n"
    )


def invalid_json() -> dict:
    return {
        "this": "is not a valid annotation result"
    }