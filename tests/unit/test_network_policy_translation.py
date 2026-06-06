from loom.driver.network_policy import (
    compute_iptables_rules,
    render_iptables_commands,
)
from loom.models.networking import Allowlist, NoNetwork, Public


def test_public_has_no_block_rules():
    plan = compute_iptables_rules(Public())
    assert plan.flush is True
    assert plan.outbound_drops == []
    assert plan.allowed_domains == []
    assert plan.allowed_cidrs == []


def test_no_network_drops_default_outbound():
    plan = compute_iptables_rules(NoNetwork())
    assert plan.flush is True
    actions = [(r.chain, r.action) for r in plan.outbound_drops]
    assert ("OUTPUT", "DROP") in actions


def test_allowlist_records_domains_and_cidrs():
    plan = compute_iptables_rules(
        Allowlist(domains=("github.com", "pypi.org"), cidrs=("10.0.0.0/8",)),
    )
    assert plan.flush is True
    assert "github.com" in plan.allowed_domains
    assert "10.0.0.0/8" in plan.allowed_cidrs
    assert any(r.action == "DROP" for r in plan.outbound_drops)


def test_render_public_is_empty():
    """Public = no enforcement → no iptables commands, so vanilla images
    (without iptables installed) still work."""
    cmds = render_iptables_commands(compute_iptables_rules(Public()))
    assert cmds == []


def test_render_no_network_sets_default_drop():
    cmds = render_iptables_commands(compute_iptables_rules(NoNetwork()))
    assert any("iptables -P OUTPUT DROP" in c for c in cmds)


def test_render_allowlist_resolves_domains_at_apply_time():
    plan = compute_iptables_rules(Allowlist(domains=("example.com",), cidrs=("10.0.0.0/8",)))
    cmds = render_iptables_commands(plan)
    assert any("getent ahosts example.com" in c for c in cmds)
    # Resolved IPs pinned to /etc/hosts so subsequent connects skip DNS.
    assert any("/etc/hosts" in c for c in cmds)
    assert any("-d 10.0.0.0/8" in c for c in cmds)
    assert any("iptables -P OUTPUT DROP" in c for c in cmds)
