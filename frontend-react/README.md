# Frontend

This is the primary frontend for the repository.

Stack:
- Vite
- React
- TypeScript
- React Query
- React Router

## Run

From the repo root:

```bash
make frontend
```

Or directly:

```bash
npm --prefix frontend-react install
npm --prefix frontend-react run dev
```

Default dev server:

```text
http://localhost:5173
```

## Backend Dependency

The app calls the FastAPI backend at:

```text
http://localhost:8000/api/v1
```

Start the backend separately:

```bash
make api
```

## Routes

- `/` — overview
- `/zones` — zone risk monitor
- `/actions` — action center
- `/health` — model health

## Notes

- The frontend still consumes API field names like `delay_risk_score` for compatibility.
- Those names map to the current backend implementation, which is centered on supply-shortage risk rather than a true delivery-delay model.
