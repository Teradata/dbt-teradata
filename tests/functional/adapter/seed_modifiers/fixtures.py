sample_seed_csv = """
id,attrA,attrB
        1,val1A,val1B
        2,val2A,val2B
        3,val3A,val3B
        4,val4A,val4B """.lstrip()

sample_model_yml="""
    config-version: 2
    seed:
        - name: sample_seed
        table_kind: SET
"""