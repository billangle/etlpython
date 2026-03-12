# DART114 /16 Networking Runbook (Ticket-Ready)

## Purpose
Provide a deterministic change procedure so Glue jobs using DART114 do not fail with subnet IP exhaustion.

## Critical Fact
AWS does not allow resizing an existing subnet. To get /16 capacity, create new subnet(s) and move the Glue connection to them.

## Change Request Summary (paste into ticket)
- Objective: Move Glue connection DART114 to a new private subnet path with /16 capacity.
- Scope: VPC networking, Glue connection, Glue job connection mapping.
- Impact: Brief deployment/change window for connection update; no data model change.
- Success criteria:
  - Glue connection test passes.
  - CNSV EXEC-SQL starts without "Number of IP addresses on subnet is 0".
  - Available IPv4 addresses in target subnet remain above agreed threshold during load.

## Inputs Required
- AWS account: 253490756794
- Region: us-east-1
- VPC ID: <fill>
- Current Glue connection name: FSA-PROD-PG-DART114
- DB endpoint/port: <fill> (Postgres expected 5432)
- Approved new CIDR block: <fill, example 10.50.0.0/16>
- Existing DB security group ID(s): <fill>
- Change window: <fill>

## Preconditions (must be true before implementation)
1. Approved non-overlapping /16 CIDR is available for the VPC (primary free space or secondary CIDR addition approved).
2. Security team approves Glue SG to DB SG ingress on 5432.
3. Route path for private subnet is defined (NAT/endpoints as required by platform standard).

## Implementation Steps

### Step 1: Confirm VPC CIDR capacity
1. Open AWS Console -> VPC -> Your VPCs -> select target VPC.
2. Check IPv4 CIDR blocks.
3. If no free /16 inside current VPC CIDR, add secondary CIDR:
   - Actions -> Edit CIDRs -> Add IPv4 CIDR.
   - Enter approved /16.
4. Record CIDR in ticket evidence.

### Step 2: Create dedicated private subnet(s) for Glue
1. VPC -> Subnets -> Create subnet.
2. VPC: target VPC.
3. Name: glue-dart114-a.
4. AZ: us-east-1a (or approved AZ).
5. IPv4 CIDR: approved /16 (or approved design if split across AZs).
6. Create at least one additional private subnet in another AZ if policy requires HA.
7. Record subnet IDs in ticket.

### Step 3: Route table association
1. VPC -> Route Tables -> select/create private route table for Glue.
2. Associate new Glue subnet(s) to this route table.
3. Ensure required routes exist:
   - local route (automatic)
   - 0.0.0.0/0 -> NAT (only if outbound internet required)
   - VPC endpoints per standard (S3, Logs, Secrets Manager, STS) if used.
4. Save screenshots/route table IDs for ticket.

### Step 4: Security controls
1. Create/confirm Glue worker security group (SG-GLUE-DART114).
2. Outbound from SG-GLUE-DART114: allow DB destination port 5432 to DB SG/CIDR per policy.
3. Inbound on DB SG: allow source SG-GLUE-DART114 on 5432.
4. Keep least privilege; do not open 0.0.0.0/0.

### Step 5: Update Glue connection DART114
1. AWS Glue -> Data Catalog -> Connections -> FSA-PROD-PG-DART114.
2. Edit network options:
   - Subnet: new Glue subnet created in Step 2.
   - Security group: SG-GLUE-DART114 (or approved equivalent).
3. Save.
4. Run "Test connection" and capture result.

### Step 6: Verify job uses DART114 connection
1. AWS Glue -> Jobs -> FSA-PROD-CNSV-EXEC-SQL.
2. Confirm Connections include FSA-PROD-PG-DART114.
3. Save job if any update required.

### Step 7: Controlled validation run
1. Start one CNSV EXEC-SQL run for known table/date.
2. Start small parallel batch (for example 3 concurrent starts).
3. Confirm no startup error with subnet IP exhaustion.
4. Capture CloudWatch/Glue run IDs in ticket.

### Step 8: Post-change observability
1. Track Available IPv4 addresses for new subnet(s).
2. Set alarms (recommended): warning and critical thresholds (example <200 and <100, adjust for org scale).
3. Document threshold values in ticket closure notes.

## Validation Checklist (ticket closure)
- [ ] New subnet(s) created and associated to private route table.
- [ ] DB SG and Glue SG rules verified.
- [ ] DART114 connection moved to new subnet(s).
- [ ] Glue connection test passed.
- [ ] CNSV EXEC-SQL run succeeded without IP exhaustion error.
- [ ] Evidence attached (subnet IDs, route table ID, SG IDs, Glue run IDs).

## Rollback Plan
1. Re-edit FSA-PROD-PG-DART114 and restore previous subnet/security group values.
2. Re-run Glue connection test.
3. Re-run single CNSV EXEC-SQL smoke test.
4. If rollback succeeds, stop and reopen change with network team.

## Known Constraints
- Existing subnet cannot be resized in place.
- NAT does not remove ENI/IP requirement for Glue in VPC.
- Glue startup still requires available private IPs in the selected subnet.

## Owner Assignment
- Network team: Steps 1-4.
- Data platform/Glue owner: Steps 5-8.
- Change manager: evidence collection and closure.
