# CSE138_Assignment3

Assignment for CSE 138

## Acknowledgements

N/A

## Citations

N/A

## Team Contributions

- Zack: Worked on KVStore and brodcasts/polling
- Ishaan: Worked on Flask endpoints and request handling
- Anthony: Worked on Causal consistency

## Mechanism Description

We tracked causal dependencies using vector clocks. Vector clocks were implemented when operations on the kv store were executed. Clocks were compared and a method was created (`is_causally_after`) to try and ensure updates were causally consistent. Unfortunately we did not get this functionality working completely.

We detected when replicas went down during a broadcast of a view/kv update. If a request failed to a repica, than it was considered down. Then, a thread was created to poll the down replica untill it was spun back up.
