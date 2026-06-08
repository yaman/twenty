import { isString } from '@sniptt/guards';
import { isPlainObject } from 'twenty-shared/utils';

import { getGraphQLErrorCode } from '@/cli/utilities/error/parse-server-error';

type ErrorWithMessage = {
  message?: string;
};

const getErrorMessage = (error: unknown): string => {
  if (isString(error)) {
    return error;
  }

  if (isPlainObject(error)) {
    const { message } = error as ErrorWithMessage;

    if (isString(message)) {
      return message;
    }
  }

  return '';
};

// Maps a known sync failure to a one-line next action the developer can take,
// so the CLI points to a recovery step instead of leaving them to guess.
export const getSyncErrorRecoveryHint = (
  error: unknown,
): string | undefined => {
  const message = getErrorMessage(error).toLowerCase();
  const code = getGraphQLErrorCode(error) ?? '';

  if (code === 'APP_NOT_INSTALLED' || message.includes('not installed')) {
    return 'Hint: run `yarn twenty dev --once` to register the app in this workspace, then retry.';
  }

  if (
    message.includes('already exists') ||
    message.includes('universalidentifier') ||
    /migration action .* failed/.test(message)
  ) {
    return 'Hint: a metadata conflict was detected. Preview the plan with `yarn twenty dev --once --dry-run`; if it persists, run `yarn twenty app:uninstall -y` then sync again.';
  }

  return undefined;
};
