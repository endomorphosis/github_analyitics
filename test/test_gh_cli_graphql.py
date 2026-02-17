import json


def test_gh_graphql_json_passes_variables_as_fields(monkeypatch):
    from github_analyitics.reporting import gh_cli

    seen = {}

    def fake_run_gh(args, cwd=None, env=None):
        seen["args"] = list(args)
        return json.dumps({"data": {"ok": True}})

    monkeypatch.setattr(gh_cli, "run_gh", fake_run_gh)

    query = "query($owner:String!, $name:String!, $after:String) { rateLimit { limit } }"
    out = gh_cli.gh_graphql_json(query, variables={"owner": "octocat", "name": "hello", "after": None})

    assert out["data"]["ok"] is True
    args = seen["args"]
    assert args[:3] == ["api", "graphql", "-f"]
    assert any(a.startswith("query=") for a in args)

    # Variables are passed as individual fields.
    assert "-f" in args
    assert "owner=octocat" in args
    assert "name=hello" in args
    assert not any(a.startswith("variables=") for a in args)
