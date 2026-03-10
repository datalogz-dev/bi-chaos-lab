# bi-chaos-lab

`bi-chaos-lab` is a manifest-driven CLI for creating disposable Power BI and Tableau sandbox estates. A manifest declares what to seed, how those estates should evolve over time, and how to tear them down cleanly when the exercise ends.

The project is aimed at BI platform testing, demos, training, migration rehearsals, and governance drills where you need realistic but controlled entropy across both vendors.

## What the CLI manages

- Power BI workspaces plus imported PBIX-backed datasets and reports
- Tableau projects plus published workbooks and datasources
- Mixed source portfolios, tracked in the manifest and carried through state metadata
- Repeatable drift waves such as refresh bursts and workbook rename noise
- Safe teardown of only tracked sandbox assets

## Core workflow

The manifest is the contract. One file describes the estate shape, source catalog, template families, and lifecycle operations.

Typical usage:

```bash
python3 -m bi_chaos_lab --manifest examples/manifest.enterprise.json validate --dry-run
python3 -m bi_chaos_lab --manifest examples/manifest.enterprise.json seed --dry-run --show-plan
python3 -m bi_chaos_lab --manifest examples/manifest.enterprise.json seed
python3 -m bi_chaos_lab --manifest examples/manifest.enterprise.json evolve
python3 -m bi_chaos_lab --manifest examples/manifest.enterprise.json teardown --dry-run
```

## Manifest model

Most manifests follow the same high-level structure:

- `name`, `random_seed`, and `safety`: run identity plus sandbox boundaries
- `platforms`: vendor-specific estate definitions for Power BI and Tableau
- `sources`: the source systems behind the BI templates
- `template_families`: reusable PBIX and TWBX/TDSX artifacts
- `domains`: business-domain scale and sprawl controls
- `scenarios`: default drift rates for `evolve`

The intent is to keep the operational commands thin and push environment detail into versioned JSON or TOML. JSON works out of the box on Python 3.9. TOML also works on Python 3.11+.

## Template families

Template families let you stamp out coherent asset groups instead of managing every workbook, report, and dataset by hand. A family defines:

- which platform the template belongs to
- which asset kind it produces
- which real source it is meant to represent
- which owned template file to upload

The generator mutates naming, placement, and state tags around those templates to create believable sprawl without hand-authoring every asset.

## Real-source strategy

The manifest is where you document which warehouse, database, file, or service each template family is supposed to represent. In this first implementation, the generator records those source mappings in state and relies on the uploaded PBIX/TWBX/TDSX artifacts to already be authored against the intended backends. Replace the placeholder artifacts before any non-dry-run `seed`.

## Safety expectations

- Use non-production tenants, sites, and credentials only.
- Keep workspace and project prefixes sandbox-only.
- Track all created assets through the generated state file.
- Prefer `seed --dry-run --show-plan` before applying changes.

## Example

Start from [`examples/manifest.enterprise.json`](/Users/derek/Documents/bi-chaos-lab/examples/manifest.enterprise.json). It shows one enterprise-shaped estate with:

- parallel Power BI and Tableau sandboxes
- reusable template families
- a mixed source catalog with warehouse, database, file, and SaaS-style references
- seed/evolve/teardown-ready safety prefixes
- placeholder PBIX and TWBX artifacts you should replace before running against a real tenant or site
