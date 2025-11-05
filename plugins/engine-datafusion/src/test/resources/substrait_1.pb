{
    "version": { "minorNumber": 24, "producer": "OpenSearch" },
    "relations": [
    {
        "root": {
        "input": {
        "aggregate": {
        "input": {
        "read": {
        "namedTable": {
        "names": ["test-index"]
        }
        }
        },
        "measures": [
        {
            "measure": {
            "functionReference": 1,
                "args": []
            }
            }
        ]
        }
        },
        "names": ["count"]
        }
        }
    ],
        "extensionUris": [
    {
        "extensionUriAnchor": 1,
            "uri": "https://github.com/substrait-io/substrait/blob/main/extensions/functions_aggregate_generic.yaml"
        }
    ],
        "extensions": [
    {
        "extensionFunction": {
        "extensionUriReference": 1,
            "functionAnchor": 1,
            "name": "count:opt"
        }
        }
    ]

    }
