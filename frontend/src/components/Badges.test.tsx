import { render, screen } from '@testing-library/react'
import { describe, expect, it } from 'vitest'
import { ExecutionBadge, PriorityBadge, StateBadge } from './Badges'

describe('badges', () => {
  it.each(['pending', 'queued', 'running', 'completed', 'failed', 'retrying', 'cancelled'] as const)(
    'renders the %s state',
    (state) => {
      render(<StateBadge state={state} />)
      expect(screen.getByTestId(`state-${state}`)).toHaveTextContent(new RegExp(state, 'i'))
    },
  )
  it('renders priority and execution outcome labels', () => {
    render(<><PriorityBadge priority="critical" /><ExecutionBadge status="timed_out" /></>)
    expect(screen.getByText('critical')).toBeInTheDocument()
    expect(screen.getByText('timed out')).toBeInTheDocument()
  })
})
