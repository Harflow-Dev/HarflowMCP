"""Generic pinned-target lifecycle conformance (S08 cases refusals, complete, nonzero, detached-resume, cancel, owner-death).

The tested engine is a caller-pinned release binary (or an exact-commit source launcher); no environment
selector changes what it executes or what it may do. Observation is by the operating system (kernel peer
PID, ancestry, kqueue exit watches) of a fake participant the harness owns, never by hooks inside the engine.
See docs/decisions/E2-lifecycle-conformance-seam.md for the open fake-backend seam.
"""
