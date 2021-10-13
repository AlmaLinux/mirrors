# coding=utf-8

MIRROR_CONFIG_SCHEMA = {
    "$schema": "http://json-schema.org/draft-04/schema#",
    "type": "object",
    "properties": {
        "cloud_type": {
            "type": "string"
        },
        "cloud_regions": {
            "type": "array",
            "items": {
                "type": "string"
            }
        },
        "name": {
            "type": "string"
        },
        "address": {
            "type": "object",
            "properties": {
                "http": {
                    "type": "string"
                },
                "https": {
                    "type": "string"
                },
                "rsync": {
                    "type": "string"
                },
                "ftp": {
                    "type": "string"
                },
            },
            "anyOf": [
                {
                    "required": [
                        "http",
                    ],
                },
                {
                    "required": [
                        "https",
                    ],
                },
            ],
        },
        "update_frequency": {
            "type": "string"
        },
        "sponsor": {
            "type": "string"
        },
        "sponsor_url": {
            "type": "string"
        },
        "email": {
            "type": "string"
        },
        "asn": {
            "oneOf": [
                {
                    "type": "string",
                },
                {
                    "type": "integer"
                }
            ]
        },
        "subnets": {
            "oneOf": [
                {
                    "type": "array",
                    "items": {
                        "type": "string"
                    }
                },
                {
                    "type": "string",
                }
            ]
        }
    },
    "required": [
        "name",
        "address",
        "update_frequency",
        "sponsor",
        "sponsor_url",
    ],
    "dependencies": {
        "cloud_type": {"required": ["cloud_regions"]}
    }
}

MAIN_CONFIG = {
    "$schema": "http://json-schema.org/draft-06/schema#",
    "$ref": "#/definitions/Welcome4",
    "definitions": {
        "Welcome4": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "allowed_outdate": {
                    "type": "string"
                },
                "mirrorlist_dir": {
                    "type": "string"
                },
                "mirrors_dir": {
                    "type": "string"
                },
                "mirrors_table": {
                    "type": "string"
                },
                "versions": {
                    "type": "array",
                    "items": {
                        "type": "string"
                    }
                },
                "duplicated_versions": {
                    "type": "array",
                    "items": {
                        "type": "string"
                    }
                },
                "required_protocols": {
                    "type": "array",
                    "items": {
                        "type": "string"
                    }
                },
                "arches": {
                    "type": "array",
                    "items": {
                        "type": "string"
                    }
                },
                "repos": {
                    "type": "array",
                    "items": {
                        "$ref": "#/definitions/Repo"
                    }
                }
            },
            "required": [
                "allowed_outdate",
                "arches",
                "duplicated_versions",
                "required_protocols",
                "mirrorlist_dir",
                "mirrors_dir",
                "mirrors_table",
                "repos",
                "versions"
            ],
            "title": "Welcome4"
        },
        "Repo": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "name": {
                    "type": "string"
                },
                "path": {
                    "type": "string"
                },
                "arches": {
                    "type": "array",
                    "items": {
                        "type": "string"
                    }
                }
            },
            "required": [
                "name",
                "path"
            ],
            "title": "Repo"
        }
    }
}
